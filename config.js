/**
 * SCM Dashboard — Google Sheets CSV 설정
 * ========================================
 *
 * Excel 데이터를 Google Sheets로 변환 → 공유(뷰어) 설정 후
 * gviz CSV endpoint로 대시보드에서 fetch합니다.
 *
 * URL 형식: https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv
 *
 * 월별 업데이트 방법:
 *   1. extract_to_csv.py 실행하여 새 CSV 생성
 *   2. Google Sheets에 새 데이터 복사/붙여넣기 또는 CSV 임포트
 *   3. 대시보드 새로고침 (gviz endpoint가 자동으로 최신 데이터 제공)
 */

const SHEETS_CONFIG = {
  // ===== 파일별 Google Sheets ID =====
  // Google Sheets "웹에 게시" CSV endpoint 사용 (CORS 지원)
  SHEETS: {
    subul_b:    { id: '1e-2a4OkGta56NZ2CxmqUKwWW63I4M99RUVJ7HEmKrOQ', name: '원재료B' },
    subul_c:    { id: '1wvnn_ya8pvszZI8IeNs90tAItkT5lncMC7To0IoYpNc', name: '제품C' },
    ta_detail:  { id: '1pxEZ8TE9ZPzmYpECNVWIkGzAXovFHIhSwPUxk5rv-ik', name: '타계정내역' },
    sales:      { id: '1-DNEyRNc9UmVxj-9816lW03mRk3vP9IFegda8TmSDT8', name: '판매현황' },
    purchase:   { id: '1qntio5uZeKYvn3dZpEFmSCgczarcfaMdp3M1otL5QvY', name: '구매현황' },
    orders:     { id: '17Nf08MdF21WS1hqag70tIs6Ph5ZUUEVlMARmh_QpMys', name: '발주서' },
    production: { id: '1fBLv3L4NSyrgJF7g_dgqhcWpBufUb9168a8sAHyWF6Q', name: '생산입고소모' },
    sku_master: { id: '1TfDoVA2U9fMYf4DBrLYdE-AGWyRavGrdwf2UHLr5j44', name: '품목코드' },
    pl_data:    { id: '1-xAyzdvTvcIslDZTtIa_ZkLfIZHSodS5KlMmnYkoftI', name: 'PL_DATA' },
  },

  // ===== Google Sheets gviz CSV endpoint =====
  csvUrl(sheetKey) {
    const cfg = this.SHEETS[sheetKey];
    if (!cfg || !cfg.id) return null;
    // Google Sheets gviz CSV endpoint (CORS 지원, 공유 설정 필요: 링크가 있는 모든 사용자 → 뷰어)
    return `https://docs.google.com/spreadsheets/d/${cfg.id}/gviz/tq?tqx=out:csv`;
  },

  // ===== 수불부 원천 Excel 컬럼 오프셋 =====
  // CSV에서 A열="월(YYYY-MM)" 추가 → 나머지 컬럼이 1칸씩 밀림
  SUBUL_MONTH_COL: 0,   // A열: 월 키 (YYYY-MM)
  SUBUL_DATA_OFFSET: 1, // B열부터 기존 Excel 컬럼 시작
};
