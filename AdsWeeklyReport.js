// Google Ads Scriptに書く内容
// これを週2回定期実行
/***** 設定 *****/
function main() {
  const CFG = {
    SPREADSHEET_URL: '', //シートのURL

    END_OFFSET_DAYS: 1,   // 昨日=1（当日は含めない）
    WINDOW_DAYS: 28,      // 直近28日

    // 検証用に週を固定したい場合（空なら無効）
    OVERRIDE_START: '',   // '2025-09-15'
    OVERRIDE_END:   '',   // '2025-09-21'

    REQUIRE_IMPRESSIONS_POSITIVE: true
  };

  runWeeklyPipeline_(CFG);
}

/***** 本体 *****/
function runWeeklyPipeline_(CFG) {
  const tz = AdsApp.currentAccount().getTimeZone();
  const ss = SpreadsheetApp.openByUrl(CFG.SPREADSHEET_URL);

  const raw = ss.getSheetByName('RawData') || ss.insertSheet('RawData');
  raw.clear();
  const weekly = ss.getSheetByName('WeeklyAgg') || ss.insertSheet('WeeklyAgg');
  weekly.clear();

  // ---- 期間（AWQLはYYYYMMDD,YYYYMMDD）
  const useOverride = CFG.OVERRIDE_START && CFG.OVERRIDE_END;
  const startDateYMD = useOverride
    ? CFG.OVERRIDE_START.replace(/-/g,'')
    : formatYmd_(daysAgo_(CFG.WINDOW_DAYS, tz), tz);
  const endDateYMD = useOverride
    ? CFG.OVERRIDE_END.replace(/-/g,'')
    : formatYmd_(daysAgo_(CFG.END_OFFSET_DAYS, tz), tz);

  // ---- WHERE（ステータスは絞らない／UI準拠）
  const whereParts = [];
  if (CFG.REQUIRE_IMPRESSIONS_POSITIVE) whereParts.push('Impressions > 0');
  const whereClause = whereParts.length ? 'WHERE ' + whereParts.join(' AND ') : '';

  const query = `
    SELECT
      Date,
      CampaignName,
      CampaignStatus,
      Impressions,
      Clicks,
      Conversions,
      Cost,
      AverageCpc
    FROM   CAMPAIGN_PERFORMANCE_REPORT
    ${whereClause}
    DURING ${startDateYMD},${endDateYMD}
  `;

  // ★ 重要：金額は“円”で返る前提で扱う（microsは使わない）
  const report = AdsApp.report(query); // オプション指定なし＝UI同等の金額表示
  const rows = report.rows();

  // ---- RawData（すべて“数値”として書く。見た目はセルの表示形式で整える）
  const rawHeader = [
    'Date','Campaign','Status',
    'Impr','Clicks','Conversions',
    'Cost(¥)','AvgCpc(¥)'
  ];
  raw.appendRow(rawHeader);

  const rawData = [];
  while (rows.hasNext()) {
    const r = rows.next();
    const date  = r['Date'];                 // YYYY-MM-DD
    const camp  = r['CampaignName'];
    const stat  = r['CampaignStatus'];
    const impr  = toInt_(r['Impressions']);
    const clk   = toInt_(r['Clicks']);
    const conv  = toFloat_(r['Conversions']);     // 小数保持
    const costY = toMoney_(r['Cost']);            // “円”を数値化（¥・, を除去）
    const avgCpcY = toMoney_(r['AverageCpc']);    // 同上

    rawData.push([date, camp, stat, impr, clk, conv, costY, avgCpcY]);
  }

  if (rawData.length) {
    raw.getRange(2,1,rawData.length,rawData[0].length).setValues(rawData);
    // 表示形式（値は数値のまま）
    raw.getRange(2,4,rawData.length,2).setNumberFormat('#,##0');       // Impr/Clicks
    raw.getRange(2,6,rawData.length,1).setNumberFormat('#,##0.00');    // Conversions
    raw.getRange(2,7,rawData.length,2).setNumberFormat('¥#,##0.00');   // Cost/AvgCpc（小数表示）
  }

  // ---- 週次（月→日）集計（円で合算：四捨五入せず小数で返す）
  const bucket = {}; // key = campaign|weekStart
  for (let i=0; i<rawData.length; i++) {
    const dateStr = rawData[i][0];
    const campaign = rawData[i][1];
    const impr = rawData[i][3];
    const clk  = rawData[i][4];
    const cv   = rawData[i][5];   // 小数CV
    const costY= rawData[i][6];   // 円

    const d = new Date(dateStr + 'T00:00:00');
    const weekStart = startOfWeekMonday_(d);
    const weekStartStr = Utilities.formatDate(weekStart, tz, 'yyyy-MM-dd');
    const key = campaign + '|' + weekStartStr;

    if (!bucket[key]) {
      bucket[key] = { campaign, weekStart: new Date(weekStart), impr:0, clk:0, cv:0, costY:0 };
    }
    bucket[key].impr  += impr;
    bucket[key].clk   += clk;
    bucket[key].cv    += cv;        // 小数合算
    bucket[key].costY += costY;     // 円合算（小数でもOK）
  }

  const weeklyRows = [];
  Object.keys(bucket).forEach(k => {
    const b = bucket[k];
    const weekEnd = new Date(b.weekStart.getTime() + 6*24*60*60*1000);

    const ctr = b.impr > 0 ? b.clk / b.impr : 0;
    const cvr = b.clk  > 0 ? b.cv  / b.clk : 0;

    // ★ 変更点：四捨五入しない（小数のまま出力）
    const costYenExact = b.costY;                             // 円：小数のまま
    const cpcExact     = b.clk > 0 ? costYenExact / b.clk : 0;
    const cpaExact     = b.cv  > 0 ? costYenExact / b.cv  : null;

    weeklyRows.push([
      Utilities.formatDate(b.weekStart, tz, 'yyyy-MM-dd'),
      Utilities.formatDate(weekEnd,     tz, 'yyyy-MM-dd'),
      b.campaign,
      b.impr,
      b.clk,
      round2_(b.cv),                        // CVは小数
      round2_(costYenExact),                // Cost(¥) 小数で返す
      pct2_(ctr),
      pct2_(cvr),
      round2_(cpcExact),                    // CPC(¥) 小数で返す
      (cpaExact!=null ? round2_(cpaExact) : '—') // CPA(¥) 小数で返す
    ]);
  });

  weeklyRows.sort((a,b) => a[0]===b[0] ? (a[2] < b[2] ? -1 : 1) : (a[0] < b[0] ? -1 : 1));

  const weeklyHeader = [
    'WeekStart(Mon)','WeekEnd(Sun)','Campaign',
    'Impressions','Clicks','CV','Cost(¥)','CTR','CVR','CPC(¥)','CPA(¥)'
  ];
  weekly.appendRow(weeklyHeader);
  if (weeklyRows.length) {
    weekly.getRange(2,1,weeklyRows.length,weeklyRows[0].length).setValues(weeklyRows);
    weekly.getRange(2,4,weeklyRows.length,2).setNumberFormat('#,##0');        // Impr/Clicks
    weekly.getRange(2,6,weeklyRows.length,1).setNumberFormat('#,##0.00');     // CV（小数）
    weekly.getRange(2,7,weeklyRows.length,1).setNumberFormat('¥#,##0.00');    // Cost（小数）
    weekly.getRange(2,10,weeklyRows.length,2).setNumberFormat('¥#,##0.00');   // CPC/CPA（小数）
  }
}

/***** Utils *****/
function daysAgo_(n, tz) {
  const todayLocal = new Date(Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd') + 'T00:00:00');
  todayLocal.setDate(todayLocal.getDate() - n);
  return todayLocal;
}
function formatYmd_(d, tz) { return Utilities.formatDate(d, tz, 'yyyyMMdd'); }
function startOfWeekMonday_(date) {
  const d = new Date(date);
  const jsDay = d.getDay();      // 0=Sun ... 6=Sat
  const diff  = (jsDay + 6) % 7; // Mon=0 ... Sun=6
  d.setHours(0,0,0,0);
  d.setDate(d.getDate() - diff);
  return d;
}
function toInt_(v){ if(v==null||v==='')return 0; return parseInt(String(v).replace(/,/g,''),10)||0; }
function toFloat_(v){ if(v==null||v==='')return 0; return parseFloat(String(v).replace(/,/g,''))||0; }
// “¥12,345.67” や “12,345.67” を数値化（円）
function toMoney_(v){
  if (v==null || v==='') return 0;
  const s = String(v).replace(/[^\d\.\-]/g,''); // ¥やカンマ除去
  return parseFloat(s) || 0;
}
function pct2_(x){ return (x*100).toFixed(2) + '%'; }
function round2_(x){ return Math.round(x*100)/100; }
