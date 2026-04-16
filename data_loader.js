/**
 * SCM Dashboard — Data Loader (JS port of scm_parser.py)
 * ========================================================
 * Google Sheets CSV → RAW_DATA 객체 생성
 *
 * 의존성: Papa Parse (CDN), config.js (SHEETS_CONFIG)
 *
 * 사용법:
 *   const RAW_DATA = await loadAllData();
 *   window.D = RAW_DATA;
 */

// ============================================================
// UTILITIES (Python n/ni/s 포팅)
// ============================================================
function _n(v) {
  if (v == null || v === '') return 0;
  const f = parseFloat(v);
  return isNaN(f) ? 0 : f;
}
function _ni(v) { return Math.round(_n(v)); }
function _s(v) { return v == null ? '' : String(v).trim(); }

function _parseDateNo(d) {
  if (!d) return '';
  return String(d).split(' ')[0].trim();
}

function _dateToMonth(d) {
  d = _parseDateNo(d);
  if (!d) return '';
  const parts = d.replace(/-/g, '/').split('/');
  if (parts.length >= 2) {
    return `${parts[0]}-${parts[1].padStart(2, '0')}`;
  }
  return '';
}

// ============================================================
// CSV FETCH & PARSE
// ============================================================
/**
 * Google Sheets 공개 CSV를 fetch + Papa Parse로 파싱
 * @param {string} sheetKey - SHEETS_CONFIG.SHEETS의 키
 * @param {object} [opts] - { headerRow: 0-based row index where data starts }
 * @returns {Promise<Array<Array<string>>>} 2D 배열 (행×열)
 */
async function fetchSheet(sheetKey, opts = {}) {
  const url = SHEETS_CONFIG.csvUrl(sheetKey);
  if (!url) {
    console.warn(`[DataLoader] No sheet ID configured for ${sheetKey}, returning empty data`);
    return [];
  }
  console.log(`[DataLoader] Fetching ${sheetKey}...`);
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Failed to fetch ${sheetKey}: ${resp.status}`);
  const text = await resp.text();
  const result = Papa.parse(text, { skipEmptyLines: true });
  if (result.errors.length) {
    console.warn(`[DataLoader] CSV parse warnings for ${sheetKey}:`, result.errors);
  }
  // opts.skipRows: 헤더/빈 행 스킵 (기본 0)
  const skip = opts.skipRows || 0;
  return result.data.slice(skip);
}

// ============================================================
// 1. CODE MAPPING (old → new SKU)
// ============================================================
/**
 * 품목코드 시트에서 사용=YES인 활성 SKU 집합 로드
 * 컬럼: A=품목코드, J(인덱스9)=사용여부
 */
function loadActiveSkus(skuRows) {
  const active = new Set();
  // 품목코드 시트: row 0,1 = 헤더 → skipRows=2 로 이미 제거된 상태
  for (const row of skuRows) {
    const code = _s(row[0]);
    const use = _s(row[9] || '');
    if (code && use.toUpperCase() === 'YES') {
      active.add(code);
    }
  }
  return active;
}

/**
 * 수불부 원재료B/제품C에서 old→new 코드 매핑 구축
 * Google Sheets 구조: A열=월, 이후 기존 Excel 컬럼 (+1 오프셋)
 */
function buildCodeMap(subulBRows, subulCRows, activeSkus) {
  const OFS = SHEETS_CONFIG.SUBUL_DATA_OFFSET; // 1
  const codeMap = {};
  const conflicts = {};

  // 원재료B: 기존 col 22 (품목코드변경전) → OFS+22 = 23
  for (const row of subulBRows) {
    const newCode = _s(row[OFS + 0]);  // 품목코드
    const oldCode = _s(row[OFS + 22]); // 품목코드(변경전)
    if (!oldCode || !newCode || oldCode === newCode) continue;
    if (activeSkus.has(oldCode)) {
      if (!conflicts[oldCode]) conflicts[oldCode] = new Set();
      conflicts[oldCode].add(newCode);
      continue;
    }
    if (codeMap[oldCode] && codeMap[oldCode] !== newCode) {
      if (!conflicts[oldCode]) conflicts[oldCode] = new Set();
      conflicts[oldCode].add(newCode);
    } else if (!codeMap[oldCode]) {
      codeMap[oldCode] = newCode;
    }
  }

  // 제품C: 기존 col 40 → OFS+40 = 41
  for (const row of subulCRows) {
    const newCode = _s(row[OFS + 0]);
    const oldCode = _s(row[OFS + 40]);
    if (!oldCode || !newCode || oldCode === newCode) continue;
    if (activeSkus.has(oldCode)) {
      if (!conflicts[oldCode]) conflicts[oldCode] = new Set();
      conflicts[oldCode].add(newCode);
      continue;
    }
    if (codeMap[oldCode] && codeMap[oldCode] !== newCode) {
      if (!conflicts[oldCode]) conflicts[oldCode] = new Set();
      conflicts[oldCode].add(newCode);
    } else if (!codeMap[oldCode]) {
      codeMap[oldCode] = newCode;
    }
  }

  // 충돌 로그
  for (const [old, news] of Object.entries(conflicts)) {
    console.warn(`[code_map 충돌/가드] ${old} → ${[...news]} → SKIP`);
  }
  console.log(`[DataLoader] code_map: ${Object.keys(codeMap).length} mappings`);
  return codeMap;
}

function mapCode(code, codeMap) {
  const c = _s(code);
  return codeMap[c] || c;
}

// ============================================================
// 2. 수불부 PARSER → mio, inv, bom
// ============================================================
/**
 * 원재료B 시트 파싱
 * Google Sheets 컬럼: A=월(YYYY-MM), B~=기존 Excel 컬럼(0-indexed)
 * 기존 Excel 원재료B 컬럼:
 *   0:품목코드, 1:품목명, 3:기초수량, 6:입고수량, 8:입고금액,
 *   9:정상출고, 10:타계정, 11:출고계, 15:출고금액계,
 *   16:기말수량, 17:기말단가, 18:기말금액
 */
function parseSubulB(rows, codeMap) {
  const OFS = SHEETS_CONFIG.SUBUL_DATA_OFFSET;
  const items = {}; // { monthKey: { sku: {...} } }

  for (const row of rows) {
    const monthKey = _s(row[SHEETS_CONFIG.SUBUL_MONTH_COL]);
    if (!monthKey || !/^\d{4}-\d{2}$/.test(monthKey)) continue;
    let sku = _s(row[OFS + 0]);
    if (!sku) continue;
    sku = mapCode(sku, codeMap);
    const name = _s(row[OFS + 1]);

    if (!items[monthKey]) items[monthKey] = {};
    items[monthKey][sku] = {
      name, itemType: '원재료',
      bq: _ni(row[OFS + 3]),
      iq: _ni(row[OFS + 6]),  ia: _n(row[OFS + 8]),
      on: _ni(row[OFS + 9]),  ot: _ni(row[OFS + 10]),
      oq: _ni(row[OFS + 11]), oa: _n(row[OFS + 15]),
      eq: _ni(row[OFS + 16]), unitCost: _n(row[OFS + 17]),
      ea: _n(row[OFS + 18]),
    };
  }
  return items;
}

/**
 * 제품C 시트 파싱 — mio 데이터 + BOM 추출
 * 기존 Excel 제품C 컬럼:
 *   0:품목코드, 1:품목명, 3:기초수량, 6:입고수량,
 *   7~21: BOM (5세트 × {자재코드, 단가, 수량})
 *   25:입고금액, 26:정상출고, 27:타계정, 28:출고계,
 *   32:출고금액, 33:기말수량, 34:기말단가, 35:기말금액
 */
function parseSubulC(rows, codeMap) {
  const OFS = SHEETS_CONFIG.SUBUL_DATA_OFFSET;
  const items = {};  // { monthKey: { sku: {...} } }
  const bomMap = {}; // { prodSku: { name, materials: [...] } } — 최신 월이 덮어씌움

  for (const row of rows) {
    const monthKey = _s(row[SHEETS_CONFIG.SUBUL_MONTH_COL]);
    if (!monthKey || !/^\d{4}-\d{2}$/.test(monthKey)) continue;
    let sku = _s(row[OFS + 0]);
    if (!sku) continue;
    sku = mapCode(sku, codeMap);
    const name = _s(row[OFS + 1]);

    // BOM 추출 (cols 7~21: 5 세트 × {자재코드, 단가, 수량})
    const bomMats = [];
    for (let bi = 0; bi < 5; bi++) {
      const base = 7 + bi * 3;
      const matCode = _s(row[OFS + base]);
      const matPrice = _n(row[OFS + base + 1]);
      const matQty = _ni(row[OFS + base + 2]);
      if (matCode) {
        bomMats.push({ mat: mapCode(matCode, codeMap), price: matPrice, qty: matQty });
      }
    }
    if (bomMats.length) {
      bomMap[sku] = { name, materials: bomMats };
    }

    if (!items[monthKey]) items[monthKey] = {};
    items[monthKey][sku] = {
      name, itemType: '제품',
      bq: _ni(row[OFS + 3]),
      iq: _ni(row[OFS + 6]),  ia: _n(row[OFS + 25]),
      on: _ni(row[OFS + 26]), ot: _ni(row[OFS + 27]),
      oq: _ni(row[OFS + 28]), oa: _n(row[OFS + 32]),
      eq: _ni(row[OFS + 33]), unitCost: _n(row[OFS + 34]),
      ea: _n(row[OFS + 35]),
    };
  }
  return { items, bomMap };
}

/**
 * 수불부 B+C 합산 → mio, invLatest, months
 */
function buildMioFromSubul(subulBItems, subulCResult) {
  const mio = {};     // { sku: { month: { iq, ia, on, ot, oq, oa, eq, ea } } }
  const invLatest = {}; // { sku: { sku_id, item_name, ... , _month } }
  const months25 = new Set();
  const months26 = new Set();

  // 모든 월-아이템 데이터를 순회
  const allMonthItems = []; // [{ monthKey, items: {sku: data} }]

  // 원재료B
  for (const [mk, skuMap] of Object.entries(subulBItems)) {
    allMonthItems.push({ monthKey: mk, items: skuMap });
  }
  // 제품C
  for (const [mk, skuMap] of Object.entries(subulCResult.items)) {
    allMonthItems.push({ monthKey: mk, items: skuMap });
  }

  // 월별 정렬 후 처리
  allMonthItems.sort((a, b) => a.monthKey.localeCompare(b.monthKey));

  for (const { monthKey, items } of allMonthItems) {
    const yr = parseInt(monthKey.slice(0, 4));
    if (yr === 2025) months25.add(monthKey);
    else months26.add(monthKey);

    for (const [sku, d] of Object.entries(items)) {
      if (!mio[sku]) mio[sku] = {};
      mio[sku][monthKey] = {
        iq: d.iq, ia: Math.round(d.ia),
        on: d.on, ot: d.ot, oq: d.oq,
        oa: Math.round(d.oa),
        eq: d.eq, ea: Math.round(d.ea),
      };

      // 최신 월 기준 inv 업데이트
      if (!invLatest[sku] || monthKey > invLatest[sku]._month) {
        invLatest[sku] = {
          sku_id: sku,
          item_name: d.name,
          item_type: d.itemType,
          qty: d.eq,
          unit_cost: Math.round(d.unitCost),
          amount: Math.round(d.ea),
          _month: monthKey,
        };
      }
    }
  }

  return {
    mio,
    invLatest,
    months25: [...months25].sort(),
    months26: [...months26].sort(),
    bomAll: subulCResult.bomMap,
  };
}

// ============================================================
// 3. 판매현황 PARSER → isd, csd, csi, csd_type, rsd, ta, ta_sum
// ============================================================
/**
 * 판매현황 시트 파싱
 * 컬럼: 0:일자-No, 1:거래처코드, 2:거래처명, 3:매장상호명, 4:품목코드,
 *       5:품목명, 6:수량, 7:단가, 8:할부금액, 9:공급가액, 10:부가세,
 *       11:합계, 12:사원명, 13:영업권코드, 14:프로젝트명
 * skipRows=2 (헤더 2행)
 */
function parseSales(rows, codeMap) {
  const records = [];

  for (const row of rows) {
    const dateNo = _s(row[0]);
    if (!dateNo || !/^\d{4}\//.test(dateNo)) continue;
    let sku = _s(row[4]);
    if (!sku) continue;
    sku = mapCode(sku, codeMap);

    records.push({
      date: _parseDateNo(dateNo),
      date_no: dateNo,
      cust_code: _s(row[1]),
      cust_name: _s(row[2]),
      shop_name: _s(row[3]),
      sku_id: sku,
      name: _s(row[5]),
      qty: _ni(row[6]),
      price: Math.round(_n(row[7])),
      supply_amt: Math.round(_n(row[9])),
      vat: Math.round(_n(row[10])),
      total: Math.round(_n(row[11])),
      rep: _s(row[12]),
      region_code: _s(row[13]),
      project: _s(row[14]),
    });
  }

  console.log(`[DataLoader] Sales: ${records.length} records (raw, no dedup)`);

  // --- isd (품목별 판매현황) ---
  const isdAgg = {};
  for (const r of records) {
    const mk = _dateToMonth(r.date);
    if (!isdAgg[r.sku_id]) isdAgg[r.sku_id] = { name: '', monthly: {} };
    isdAgg[r.sku_id].name = r.name;
    if (!isdAgg[r.sku_id].monthly[mk]) isdAgg[r.sku_id].monthly[mk] = { qty: 0, amt: 0 };
    isdAgg[r.sku_id].monthly[mk].qty += r.qty;
    isdAgg[r.sku_id].monthly[mk].amt += r.supply_amt;
  }
  const isd = Object.entries(isdAgg).map(([k, v]) => ({
    sku_id: k, name: v.name,
    type: k.startsWith('C-') ? '제품(C)' : k.startsWith('B-') ? '원재료(B)' : '기타',
    monthly: v.monthly,
  }));

  // --- csd (거래처별 매출) ---
  const csdAgg = {};
  for (const r of records) {
    const mk = _dateToMonth(r.date);
    if (!csdAgg[r.cust_name]) csdAgg[r.cust_name] = { code: '', monthly: {} };
    csdAgg[r.cust_name].code = r.cust_code;
    if (!csdAgg[r.cust_name].monthly[mk]) csdAgg[r.cust_name].monthly[mk] = { qty: 0, amt: 0, cnt: 0 };
    csdAgg[r.cust_name].monthly[mk].qty += r.qty;
    csdAgg[r.cust_name].monthly[mk].amt += r.supply_amt;
    csdAgg[r.cust_name].monthly[mk].cnt += 1;
  }
  const csd = Object.entries(csdAgg).map(([k, v]) => ({
    cust_code: v.code, cust_name: k, name: k, monthly: v.monthly,
  }));

  // --- csi (거래처×품목 교차) ---
  const csiAgg = {};
  for (const r of records) {
    const mk = _dateToMonth(r.date);
    const key = `${r.sku_id}||${r.cust_name}`;
    if (!csiAgg[key]) csiAgg[key] = { ic: r.sku_id, c: r.cust_name, n: '', ml: {} };
    csiAgg[key].n = r.name;
    if (!csiAgg[key].ml[mk]) csiAgg[key].ml[mk] = { q: 0, a: 0 };
    csiAgg[key].ml[mk].q += r.qty;
    csiAgg[key].ml[mk].a += r.supply_amt;
  }
  const csi = Object.values(csiAgg);

  // --- csd_type (유형별 거래처 매출) ---
  const csdTypeAgg = {};
  for (const r of records) {
    const mk = _dateToMonth(r.date);
    const tp = r.sku_id.startsWith('C-') ? 'C' : r.sku_id.startsWith('B-') ? 'B' : 'S';
    const key = `${r.cust_name}||${tp}`;
    if (!csdTypeAgg[key]) csdTypeAgg[key] = { tp, name: r.cust_name, monthly: {} };
    if (!csdTypeAgg[key].monthly[mk]) csdTypeAgg[key].monthly[mk] = { qty: 0, amt: 0, cnt: 0 };
    csdTypeAgg[key].monthly[mk].qty += r.qty;
    csdTypeAgg[key].monthly[mk].amt += r.supply_amt;
    csdTypeAgg[key].monthly[mk].cnt += 1;
  }
  const csdType = Object.values(csdTypeAgg);

  // --- ta (sales audit trail) ---
  const ta = records.map(r => ({
    date: r.date, cust: r.cust_name, shop: r.shop_name,
    sku_id: r.sku_id, name: r.name, qty: r.qty,
    amt: r.supply_amt, total: r.total, rep: r.rep, project: r.project,
  }));

  // --- ta_sum (월별 거래 요약) ---
  const taSumAgg = {};
  for (const r of records) {
    const mk = _dateToMonth(r.date);
    if (!taSumAgg[mk]) taSumAgg[mk] = { qty: 0, amt: 0, cnt: 0, custs: new Set() };
    taSumAgg[mk].qty += r.qty;
    taSumAgg[mk].amt += r.supply_amt;
    taSumAgg[mk].cnt += 1;
    taSumAgg[mk].custs.add(r.cust_name);
  }
  const taSumArr = Object.entries(taSumAgg)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => ({ month: k, qty: v.qty, amt: v.amt, cnt: v.cnt, cust_cnt: v.custs.size }));

  // --- rsd (사원별 매출) ---
  const rsdAgg = {};
  for (const r of records) {
    const rep = r.rep || '미지정';
    const mk = _dateToMonth(r.date);
    if (!rsdAgg[rep]) rsdAgg[rep] = { qty: 0, amt: 0, cnt: 0, custs: new Set(), monthly: {} };
    rsdAgg[rep].qty += r.qty;
    rsdAgg[rep].amt += r.supply_amt;
    rsdAgg[rep].cnt += 1;
    rsdAgg[rep].custs.add(r.cust_name);
    rsdAgg[rep].monthly[mk] = (rsdAgg[rep].monthly[mk] || 0) + r.supply_amt;
  }
  const rsd = Object.entries(rsdAgg).map(([k, v]) => ({
    rep: k, name: k, qty: v.qty, amt: v.amt, cnt: v.cnt,
    cust_cnt: v.custs.size, monthly: v.monthly,
  }));

  return { isd, csd, csi, csdType, ta, taSumArr, rsd, records };
}

// ============================================================
// 4. 구매현황 PARSER → purch25, purch26
// ============================================================
/**
 * 구매현황 시트 파싱
 * 컬럼: 0:일자-No, 1:품목코드, 2:품목명, 3:수량, 4:단가, 5:공급가액,
 *       6:부가세, 7:합계, 8:거래처명
 * skipRows=2 (헤더 2행)
 */
function parsePurchase(rows, codeMap) {
  const records = [];
  for (const row of rows) {
    const dateNo = _s(row[0]);
    // 월소계 'YYYY/MM 계' 제외 — 날짜 형식만 허용
    if (!dateNo || !/^\d{4}\/\d{2}\/\d{2}/.test(dateNo)) continue;
    let sku = _s(row[1]);
    if (!sku) continue;
    sku = mapCode(sku, codeMap);
    records.push({
      date: _parseDateNo(dateNo),
      sku_id: sku,
      name: _s(row[2]),
      qty: _ni(row[3]),
      price: Math.round(_n(row[4])),
      amount: Math.round(_n(row[5])),
      vendor: _s(row[8]),
    });
  }

  const purch25 = records.filter(r => r.date.startsWith('2025'));
  const purch26 = records.filter(r => r.date.startsWith('2026'));
  console.log(`[DataLoader] Purchase: ${purch25.length} (2025), ${purch26.length} (2026)`);
  return { purch25, purch26 };
}

// ============================================================
// 5. 발주서 PARSER → po, vdd
// ============================================================
/**
 * 발주서 시트 파싱
 * 컬럼: 0:일자-No, 1:거래처명, 2:담당자명, 3:품목명[규격명],
 *       4:희망납기일, 5:발주금액합계, 6:진행상태
 * skipRows=2
 */
function parseOrders(rows) {
  const records = [];
  for (const row of rows) {
    const dateNo = _s(row[0]);
    if (!dateNo || !/^\d{4}\//.test(dateNo)) continue;
    records.push({
      date_no: dateNo,
      vendor: _s(row[1]),
      manager: _s(row[2]),
      item_name: _s(row[3]),
      due_date: _s(row[4]),
      amount: Math.round(_n(row[5])),
      status: _s(row[6]),
    });
  }
  console.log(`[DataLoader] Orders: ${records.length} records`);

  // vdd (업체별 집계)
  const vddAgg = {};
  for (const r of records) {
    const mk = _dateToMonth(r.date_no);
    if (!vddAgg[r.vendor]) vddAgg[r.vendor] = { total_amt: 0, total_cnt: 0, monthly: {} };
    vddAgg[r.vendor].total_amt += r.amount;
    vddAgg[r.vendor].total_cnt += 1;
    if (!vddAgg[r.vendor].monthly[mk]) vddAgg[r.vendor].monthly[mk] = { amt: 0, cnt: 0 };
    vddAgg[r.vendor].monthly[mk].amt += r.amount;
    vddAgg[r.vendor].monthly[mk].cnt += 1;
  }
  const vdd = Object.entries(vddAgg).map(([k, v]) => ({
    name: k, total_amt: v.total_amt, total_cnt: v.total_cnt, monthly: v.monthly,
  }));

  return { po: records, vdd };
}

// ============================================================
// 6. 생산 PARSER → pr, pr26
// ============================================================
/**
 * 생산입고소모 시트 파싱
 * 컬럼: 0:일자-No, 1:생산품목코드, 2:생산품목명, 3:소모품목코드,
 *       4:소모품목명, 5:생산수량, 6:표준소모, 7:실제소모
 * skipRows=2
 */
function parseProduction(rows, codeMap) {
  const prAgg = {};  // { "date||sku": { name, qty } }
  const consRecords = [];

  for (const row of rows) {
    const dateNo = _s(row[0]);
    if (!dateNo || !/^\d{4}\//.test(dateNo)) continue;
    const prodSku = mapCode(_s(row[1]), codeMap);
    const consSku = mapCode(_s(row[3]), codeMap);
    const prodQty = _ni(row[5]);
    const consQty = _ni(row[7]); // 실제소모

    if (prodSku && prodQty > 0) {
      const key = `${_parseDateNo(dateNo)}||${prodSku}`;
      if (!prAgg[key]) prAgg[key] = { date: _parseDateNo(dateNo), sku_id: prodSku, name: _s(row[2]), qty: 0 };
      prAgg[key].qty = Math.max(prAgg[key].qty, prodQty); // BOM 라인 중복 → max
    }
    if (consSku && consQty > 0) {
      consRecords.push({
        date: _parseDateNo(dateNo),
        prod_sku: prodSku,
        sku_id: consSku,
        name: _s(row[4]),
        qty: consQty,
      });
    }
  }

  const allPr = Object.values(prAgg);
  const pr = allPr.filter(r => r.date.startsWith('2025'));
  const pr26 = allPr.filter(r => r.date.startsWith('2026'));
  console.log(`[DataLoader] Production: ${pr.length} (2025), ${pr26.length} (2026)`);
  return { pr, pr26, consRecords };
}

// ============================================================
// 7. 품목코드 PARSER → sku
// ============================================================
/**
 * 품목코드 시트 파싱
 * 컬럼: 0:품목코드, 1:카테고리, 2:품목명, 3:유형, 6:등급
 * skipRows=2
 */
function parseSkuMaster(rows, codeMap) {
  const skus = [];
  for (const row of rows) {
    let code = _s(row[0]);
    if (!code) continue;
    code = mapCode(code, codeMap);
    skus.push({
      id: code,
      cat: _s(row[1]),
      name: _s(row[2]),
      type: _s(row[3]),
      grade: _s(row[6]),
    });
  }
  console.log(`[DataLoader] SKU master: ${skus.length} items`);
  return skus;
}

// ============================================================
// 8. 타계정내역 PARSER → ta, ta_sum
// ============================================================
/**
 * 타계정내역 시트 파싱 (수불부 내 타계정내역 통합)
 * Google Sheets 컬럼 (월 컬럼 추가):
 *   A=월(YYYY-MM), B=출고일, C=구분, D=품목코드, E=품목명,
 *   F=수량, G=단가, H=금액, I=세부내역, J=사용처, K=계정명
 * skipRows=4 (원래 수불부 타계정내역은 row5부터 데이터)
 */
function parseTaDetail(rows, codeMap) {
  const ta = [];
  const acctAgg = {};

  for (const row of rows) {
    // B열(인덱스1) = 출고일
    const rawDate = _s(row[1]);
    if (!rawDate || !/^\d{4}/.test(rawDate)) continue;

    const dStr = _parseDateNo(rawDate);
    let sku = _s(row[3]);
    if (!sku) continue;
    sku = mapCode(sku, codeMap);

    const itemType = _s(row[2]);
    const nm = _s(row[4]);
    const qty = _ni(row[5]);
    const uc = Math.round(_n(row[6]));
    const amt = Math.round(_n(row[7]));
    const detail = _s(row[8]);
    const loc = _s(row[9]);
    const acct = _s(row[10]) || '(미분류)';

    ta.push({ d: dStr, a: amt, q: qty, ac: acct, nm, sk: sku, loc, uc, dt: detail, t: itemType });

    if (!acctAgg[acct]) acctAgg[acct] = { qty: 0, amt: 0 };
    acctAgg[acct].qty += qty;
    acctAgg[acct].amt += amt;
  }

  ta.sort((a, b) => a.d.localeCompare(b.d));
  const taSumOt = Object.entries(acctAgg)
    .sort(([, a], [, b]) => b.amt - a.amt)
    .map(([k, v]) => ({ ac: k, qty: v.qty, amt: v.amt }));

  console.log(`[DataLoader] 타계정: ${ta.length} records, ${taSumOt.length} account types`);
  return { taOt: ta, taOtSum: taSumOt };
}

// ============================================================
// 9. PL_DATA PARSER (손익계산서)
// ============================================================
/**
 * PL_DATA 시트 파싱
 * 컬럼: 0:월(YYYY-MM), 1:B매출, 2:B원가, 3:C매출, 4:C원가, 5:C매입액, 6:C타계정
 */
function parsePLData(rows) {
  const plData = {};
  for (const row of rows) {
    const month = _s(row[0]);
    if (!month || !/^\d{4}-\d{2}$/.test(month)) continue;
    plData[month] = {
      b: { rev: Math.round(_n(row[1])), cogs: Math.round(_n(row[2])) },
      c: {
        rev: Math.round(_n(row[3])),
        cogs: Math.round(_n(row[4])),
        purch: Math.round(_n(row[5])),
        otherAcct: Math.round(_n(row[6])),
      },
    };
  }
  console.log(`[DataLoader] PL_DATA: ${Object.keys(plData).length} months`);
  return plData;
}

// ============================================================
// 10. AGGREGATION FUNCTIONS
// ============================================================

/** mio에 수불부 없는 월의 데이터를 거래 데이터로 채워넣기 */
function fillMissingMioMonths(mio, invLatest, bomAll, months26, purch26, salesRecords, pr26, consRecords, codeMap) {
  // 거래데이터에 있는 2026 월 찾기
  const txMonths = new Set();
  for (const r of (purch26 || [])) { const m = _dateToMonth(r.date); if (m) txMonths.add(m); }
  for (const r of salesRecords) { if (r.date.startsWith('2026')) { const m = _dateToMonth(r.date); if (m) txMonths.add(m); } }
  for (const r of (pr26 || [])) { const m = _dateToMonth(r.date); if (m) txMonths.add(m); }

  const mioMonths = new Set();
  for (const skuIo of Object.values(mio)) {
    for (const m of Object.keys(skuIo)) { if (m.startsWith('2026')) mioMonths.add(m); }
  }

  const missing = [...txMonths].filter(m => !mioMonths.has(m)).sort();
  if (!missing.length) return months26;

  console.log(`[DataLoader] Filling missing mio months: ${missing}`);

  // 룩업 구조 빌드
  const purchByMs = {};
  for (const r of (purch26 || [])) {
    const m = _dateToMonth(r.date);
    const key = `${m}||${r.sku_id}`;
    if (!purchByMs[key]) purchByMs[key] = { qty: 0, amt: 0 };
    purchByMs[key].qty += r.qty;
    purchByMs[key].amt += (r.amount || 0);
  }

  const salesByMs = {};
  for (const r of salesRecords) {
    if (!r.date.startsWith('2026')) continue;
    const m = _dateToMonth(r.date);
    const key = `${m}||${r.sku_id}`;
    if (!salesByMs[key]) salesByMs[key] = { qty: 0, amt: 0 };
    salesByMs[key].qty += r.qty;
    salesByMs[key].amt += (r.supply_amt || 0);
  }

  const prodByMs = {};
  for (const r of (pr26 || [])) {
    const m = _dateToMonth(r.date);
    const key = `${m}||${r.sku_id}`;
    prodByMs[key] = Math.max(prodByMs[key] || 0, r.qty);
  }

  const consByMs = {};
  for (const r of (consRecords || [])) {
    if (!r.date.startsWith('2026')) continue;
    const m = _dateToMonth(r.date);
    const key = `${m}||${r.sku_id}`;
    consByMs[key] = (consByMs[key] || 0) + r.qty;
  }

  // 아이템 유형 판별
  const itemTypes = {};
  for (const [sku, d] of Object.entries(invLatest)) itemTypes[sku] = d.item_type || '';
  for (const r of (purch26 || [])) { if (!itemTypes[r.sku_id]) itemTypes[r.sku_id] = '원재료'; }
  for (const r of (pr26 || [])) { if (!itemTypes[r.sku_id]) itemTypes[r.sku_id] = '제품'; }

  for (const mm of missing) {
    const yr = parseInt(mm.slice(0, 4));
    const mo = parseInt(mm.slice(5));
    const prevM = mo === 1 ? `${yr - 1}-12` : `${yr}-${String(mo - 1).padStart(2, '0')}`;

    // 활성 SKU 수집
    const activeSkus = new Set();
    for (const key of Object.keys(purchByMs)) { if (key.startsWith(mm + '||')) activeSkus.add(key.split('||')[1]); }
    for (const key of Object.keys(salesByMs)) { if (key.startsWith(mm + '||')) activeSkus.add(key.split('||')[1]); }
    for (const key of Object.keys(prodByMs)) { if (key.startsWith(mm + '||')) activeSkus.add(key.split('||')[1]); }
    for (const key of Object.keys(consByMs)) { if (key.startsWith(mm + '||')) activeSkus.add(key.split('||')[1]); }
    for (const [sku, io] of Object.entries(mio)) {
      if (io[prevM] && io[prevM].eq !== 0) activeSkus.add(sku);
    }

    for (const sku of activeSkus) {
      const itype = itemTypes[sku] || '';
      const prevData = (mio[sku] || {})[prevM] || {};
      const prevEq = prevData.eq || 0;
      const prevEa = prevData.ea || 0;
      const prevUc = prevEq !== 0 ? (prevEa / prevEq) : 0;

      let iq, ia, on, ot = 0;
      const key = `${mm}||${sku}`;

      if (itype === '원재료') {
        iq = (purchByMs[key] || {}).qty || 0;
        ia = (purchByMs[key] || {}).amt || 0;
        on = consByMs[key] || 0;
      } else if (itype === '제품') {
        iq = prodByMs[key] || 0;
        ia = Math.round(iq * prevUc) || 0;
        on = (salesByMs[key] || {}).qty || 0;
      } else {
        iq = ((purchByMs[key] || {}).qty || 0) + (prodByMs[key] || 0);
        ia = (purchByMs[key] || {}).amt || 0;
        on = ((salesByMs[key] || {}).qty || 0) + (consByMs[key] || 0);
      }

      const oq = on + Math.abs(ot);
      const oa = Math.round(oq * prevUc) || 0;
      const eq = prevEq + iq - oq;
      const ea = Math.round(eq * prevUc) || 0;

      if (!mio[sku]) mio[sku] = {};
      mio[sku][mm] = { iq, ia: Math.round(ia), on, ot, oq, oa, eq, ea };

      // invLatest 갱신
      if (invLatest[sku]) {
        if (mm > (invLatest[sku]._month || '')) {
          invLatest[sku].qty = eq;
          invLatest[sku].amount = ea;
          invLatest[sku].unit_cost = prevUc ? Math.round(prevUc) : invLatest[sku].unit_cost;
          invLatest[sku]._month = mm;
        }
      } else {
        let name = '';
        for (const r of (purch26 || [])) { if (r.sku_id === sku) { name = r.name || ''; break; } }
        if (!name) { for (const r of salesRecords) { if (r.sku_id === sku) { name = r.name || ''; break; } } }
        invLatest[sku] = {
          sku_id: sku, item_name: name, item_type: itype || '기타',
          qty: eq, unit_cost: prevUc ? Math.round(prevUc) : 0, amount: ea, _month: mm,
        };
      }
    }
  }

  return [...new Set([...months26, ...missing])].sort();
}

/** inv 배열 구축 */
function buildInv(invLatest, mio, months25, months26, salesRecords) {
  const salesBySku25 = {};
  const salesBySku26 = {};
  for (const r of salesRecords) {
    const yr = r.date.slice(0, 4);
    if (yr === '2025') {
      salesBySku25[r.sku_id] = (salesBySku25[r.sku_id] || 0) + r.qty;
    } else if (yr === '2026') {
      if (!salesBySku26[r.sku_id]) salesBySku26[r.sku_id] = { qty: 0, amt: 0 };
      salesBySku26[r.sku_id].qty += r.qty;
      salesBySku26[r.sku_id].amt += r.supply_amt;
    }
  }

  const inv = [];
  for (const [sku, d] of Object.entries(invLatest)) {
    const io = mio[sku] || {};
    const sumField = (months, field) => months.reduce((acc, m) => acc + ((io[m] || {})[field] || 0), 0);

    inv.push({
      sku_id: sku,
      item_name: d.item_name,
      item_type: d.item_type,
      qty: d.qty,
      unit_cost: d.unit_cost,
      amount: d.amount,
      y25_io: {
        'in': sumField(months25, 'iq'), out: sumField(months25, 'oq'),
        on: sumField(months25, 'on'), ot: sumField(months25, 'ot'),
      },
      y25_sales: salesBySku25[sku] || 0,
      y26_io: {
        iq: sumField(months26, 'iq'), on: sumField(months26, 'on'),
        ot: sumField(months26, 'ot'), oq: sumField(months26, 'oq'), bd: 0,
      },
      y26_sales: salesBySku26[sku] || { qty: 0, amt: 0 },
    });
  }
  return inv;
}

/** mt (월별 합계) 구축 */
function buildMonthlyTotals(mio, months) {
  const mt = {};
  for (const m of months) {
    let iq = 0, ia = 0, on = 0, ot = 0, oq = 0, oa = 0, eq = 0, ea = 0;
    for (const io of Object.values(mio)) {
      const d = io[m];
      if (!d) continue;
      iq += d.iq; ia += d.ia; on += d.on; ot += d.ot;
      oq += d.oq; oa += d.oa; eq += d.eq; ea += d.ea;
    }
    mt[m] = { iq, ia: Math.round(ia), on, ot, oq, oa: Math.round(oa), eq, ea: Math.round(ea) };
  }
  return mt;
}

/** ms (월별 요약 배열) 구축 */
function buildMs(mt, months) {
  return months.map(m => {
    const d = mt[m] || {};
    return {
      month: m,
      in_qty: d.iq || 0, in_amt: d.ia || 0,
      out_qty: d.oq || 0, out_amt: d.oa || 0,
      end_qty: d.eq || 0, end_amt: d.ea || 0,
      amount: d.oa || 0,
    };
  });
}

/** bom 배열 구축 */
function buildBom(bomAll, codeMap, invLatest) {
  const bom = [];
  const seen = new Set();
  for (const [prodSku, bd] of Object.entries(bomAll)) {
    for (const mat of bd.materials) {
      const key = `${prodSku}||${mat.mat}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const matName = invLatest[mat.mat] ? invLatest[mat.mat].item_name : '';
      bom.push({
        parent_sku: prodSku,
        parent_name: bd.name,
        child_sku: mat.mat,
        child_name: matName,
        qty: mat.qty,
        price: Math.round(mat.price),
      });
    }
  }
  return bom;
}

/** bom_demand (BOM 기반 원재료 수요) 구축 */
function buildBomDemand(bomAll, mio, months26) {
  const demand = {};
  for (const [prodSku, bd] of Object.entries(bomAll)) {
    const io = mio[prodSku] || {};
    for (const m of months26) {
      const d = io[m] || {};
      const prodOut = (d.on || 0) + (d.ot || 0);
      if (prodOut > 0) {
        for (const mat of bd.materials) {
          if (!demand[mat.mat]) demand[mat.mat] = {};
          demand[mat.mat][m] = (demand[mat.mat][m] || 0) + prodOut * mat.qty;
        }
      }
    }
  }
  return demand;
}

/** hist (SKU별 히스토리) 구축 */
function buildHist(mio, purch25, purch26, pr, pr26) {
  const purchBySku = {};
  for (const r of (purch26 || [])) {
    if (!purchBySku[r.sku_id]) purchBySku[r.sku_id] = [];
    purchBySku[r.sku_id].push({
      date: r.date, vendor: r.vendor, qty: r.qty, price: r.price, amount: r.amount,
    });
  }

  const prodBySku = {};
  for (const r of (pr26 || [])) {
    if (!prodBySku[r.sku_id]) prodBySku[r.sku_id] = [];
    prodBySku[r.sku_id].push({ date: r.date, qty: r.qty });
  }

  const hist = {};
  const allSkus = new Set([...Object.keys(mio), ...Object.keys(purchBySku), ...Object.keys(prodBySku)]);
  for (const sku of allSkus) {
    const purchases = purchBySku[sku] || [];
    const production = prodBySku[sku] || [];
    if (purchases.length || production.length) {
      hist[sku] = { sales: [], purchases, production };
    }
  }
  return hist;
}

// ============================================================
// 11. MAIN LOADER — 전체 파이프라인
// ============================================================

/**
 * 모든 Google Sheets 데이터를 fetch하고 RAW_DATA를 빌드하는 메인 함수
 * @param {function} [onProgress] - 진행상황 콜백 (msg, pct)
 * @returns {Promise<object>} RAW_DATA 객체
 */
async function loadAllData(onProgress) {
  const prog = onProgress || (() => {});

  // ─── Step 1: 모든 시트 fetch (병렬) ───
  prog('Google Sheets 데이터 로딩 중...', 5);
  const [subulBRows, subulCRows, taRows, salesRows, purchRows, orderRows, prodRows, skuRows, plRows] =
    await Promise.all([
      fetchSheet('subul_b',    { skipRows: 8 }),   // 수불부 원재료B: row 9부터 데이터
      fetchSheet('subul_c',    { skipRows: 8 }),   // 수불부 제품C: row 9부터 데이터
      fetchSheet('ta_detail',  { skipRows: 4 }),   // 타계정내역: row 5부터
      fetchSheet('sales',      { skipRows: 2 }),   // 판매현황: row 3부터
      fetchSheet('purchase',   { skipRows: 2 }),   // 구매현황: row 3부터
      fetchSheet('orders',     { skipRows: 2 }),   // 발주서: row 3부터
      fetchSheet('production', { skipRows: 2 }),   // 생산: row 3부터
      fetchSheet('sku_master', { skipRows: 2 }),   // 품목코드: row 3부터
      fetchSheet('pl_data',    { skipRows: 1 }),   // PL_DATA: row 2부터 (헤더 1행)
    ]);
  prog('데이터 로딩 완료, 파싱 중...', 30);

  // ─── Step 2: 코드 매핑 ───
  prog('SKU 코드 매핑 구축 중...', 35);
  const activeSkus = loadActiveSkus(skuRows);
  const codeMap = buildCodeMap(subulBRows, subulCRows, activeSkus);

  // ─── Step 3: 수불부 파싱 ───
  prog('수불부 데이터 파싱 중...', 40);
  const subulBItems = parseSubulB(subulBRows, codeMap);
  const subulCResult = parseSubulC(subulCRows, codeMap);
  let { mio, invLatest, months25, months26, bomAll } = buildMioFromSubul(subulBItems, subulCResult);

  // ─── Step 4: 판매현황 파싱 ───
  prog('판매현황 파싱 중...', 50);
  const salesResult = parseSales(salesRows, codeMap);

  // ─── Step 5: 구매현황 파싱 ───
  prog('구매/발주/생산 파싱 중...', 60);
  const purchResult = parsePurchase(purchRows, codeMap);
  const orderResult = parseOrders(orderRows);
  const prodResult = parseProduction(prodRows, codeMap);

  // ─── Step 6: 품목코드 파싱 ───
  prog('마스터 데이터 처리 중...', 70);
  const skuMaster = parseSkuMaster(skuRows, codeMap);

  // ─── Step 7: 누락 월 채우기 ───
  prog('누락 월 데이터 보정 중...', 75);
  months26 = fillMissingMioMonths(
    mio, invLatest, bomAll, months26,
    purchResult.purch26, salesResult.records, prodResult.pr26, prodResult.consRecords, codeMap
  );

  // ─── Step 8: 집계 ───
  prog('집계 데이터 생성 중...', 80);
  const inv = buildInv(invLatest, mio, months25, months26, salesResult.records);
  const mt25 = buildMonthlyTotals(mio, months25);
  const mt26 = buildMonthlyTotals(mio, months26);
  const ms25 = buildMs(mt25, months25);
  const ms = buildMs(mt26, months26);
  const bom = buildBom(bomAll, codeMap, invLatest);
  const bomDemand = buildBomDemand(bomAll, mio, months26);
  const hist = buildHist(mio, purchResult.purch25, purchResult.purch26, prodResult.pr, prodResult.pr26);

  // ─── Step 9: 타계정 파싱 ───
  prog('타계정 데이터 처리 중...', 85);
  const taResult = parseTaDetail(taRows, codeMap);

  // ─── Step 10: PL_DATA 파싱 ───
  prog('손익계산서 데이터 처리 중...', 88);
  const plData = parsePLData(plRows);

  // ─── Step 11: B-/C- 필터링 ───
  prog('데이터 필터링 중...', 90);
  const invFiltered = inv.filter(i => i.sku_id.startsWith('B-') || i.sku_id.startsWith('C-'));
  const mioFiltered = {};
  for (const [k, v] of Object.entries(mio)) {
    if (k.startsWith('B-') || k.startsWith('C-')) mioFiltered[k] = v;
  }
  console.log(`[DataLoader] Filtered B-/C-: inv ${inv.length}→${invFiltered.length}, mio ${Object.keys(mio).length}→${Object.keys(mioFiltered).length}`);

  // ─── Step 12: RAW_DATA 조립 ───
  prog('최종 데이터 조립 중...', 95);
  const RAW_DATA = {
    _meta: {
      generated: new Date().toISOString(),
      months_2025: months25,
      months_2026: months26,
      source: 'data_loader.js (Google Sheets)',
    },
    inv: invFiltered,
    sku: skuMaster,
    bom,
    bom_demand: bomDemand,
    mio: mioFiltered,
    isd: salesResult.isd,
    csd: salesResult.csd,
    csi: salesResult.csi,
    csd_type: salesResult.csdType,
    rsd: salesResult.rsd,
    ta: taResult.taOt,
    ta_sum: taResult.taOtSum,
    sales_tx: salesResult.ta,
    sales_tx_sum: salesResult.taSumArr,
    ms,
    ms25,
    mt25,
    mt26,
    po: orderResult.po,
    vdd: orderResult.vdd,
    purch25: purchResult.purch25,
    purch26: purchResult.purch26,
    pr: prodResult.pr,
    pr26: prodResult.pr26,
    hist,
    // ★ PL_DATA — 기존 하드코딩 대체
    pl_data: plData,
  };

  prog('완료!', 100);
  console.log('[DataLoader] RAW_DATA built successfully:', {
    inv: invFiltered.length,
    mio: Object.keys(mioFiltered).length,
    bom: bom.length,
    isd: salesResult.isd.length,
    csd: salesResult.csd.length,
    sales_tx: salesResult.ta.length,
    months: `${months25[0]}~${months26[months26.length - 1]}`,
  });

  return RAW_DATA;
}

// ============================================================
// 12. 캐시 + 오프라인 FALLBACK
// ============================================================
const CACHE_KEY = 'scm_dashboard_cache';
const CACHE_TTL = 24 * 60 * 60 * 1000; // 24시간

function saveToCache(data) {
  try {
    const payload = { ts: Date.now(), data };
    localStorage.setItem(CACHE_KEY, JSON.stringify(payload));
    console.log('[DataLoader] Cached to localStorage');
  } catch (e) {
    console.warn('[DataLoader] Cache save failed:', e.message);
  }
}

function loadFromCache() {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const { ts, data } = JSON.parse(raw);
    if (Date.now() - ts > CACHE_TTL) {
      console.log('[DataLoader] Cache expired');
      return null;
    }
    console.log('[DataLoader] Loaded from cache');
    return data;
  } catch (e) {
    return null;
  }
}

/**
 * 메인 진입점: 데이터 로드 (온라인 → 캐시 fallback)
 */
async function initData(onProgress) {
  try {
    const data = await loadAllData(onProgress);
    saveToCache(data);
    return data;
  } catch (err) {
    console.error('[DataLoader] Online load failed:', err);
    const cached = loadFromCache();
    if (cached) {
      console.warn('[DataLoader] Using cached data (offline fallback)');
      if (onProgress) onProgress('캐시 데이터 사용 중 (오프라인)', 100);
      return cached;
    }
    throw new Error('데이터를 불러올 수 없습니다. 인터넷 연결을 확인해 주세요.');
  }
}
