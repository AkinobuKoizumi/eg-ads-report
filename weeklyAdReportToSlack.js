/***** Apps Script（Sheet側）
 * 週次要約 → Slack通知（直近週主役・ポジティブ基調）
 * ＋ 課題判定（Baselines: campaign別→global→デフォルト）
 * ＋ ナレッジ参照（DocIndex + ReportIndex を統合スコアで抽出）
 * ＋ 文体参照（StyleIndex：例文は参照のみ／中身はマスク）
 * ＋ 数値ガード：
 *    - KPI（結果セクション）はGASで生成し、AI出力後に強制差し替え
 *    - AIには「許可された数値以外は出力禁止」を明示し、許可リストを付与
 *    - 許可リストはWeeklyAgg（＋任意でRawData）由来のみ
 * 生成AIは 5→6 の 1回のみ
 * 出力体裁：見出し=Slack絵文字ショートコード、箇条書き=" • "（半角スペース+中黒）
 *****/

// ===== エントリーポイント =====
function main() { pipelineAndNotify(); }

// ===== 設定 =====
const CFG = {
  // OpenAI
  OPENAI_MODEL: 'gpt-4o-mini',
  OPENAI_TEMPERATURE: 0.2,
  OPENAI_MAX_TOKENS: 900,

  // レポート方針
  LOOKBACK_WEEKS: 4,                 // GPTに渡す直近週数（分析は最新週主役）

  // シート名
  WEEKLY_SHEET: 'WeeklyAgg',
  RAW_SHEET: 'RawData',              // 任意：あれば数値許可リストに取り込む
  DOCINDEX_SHEET: 'DocIndex',        // 共通ナレッジ
  REPORTINDEX_SHEET: 'ReportIndex',  // アカウント固有レポ知見
  STYLEINDEX_SHEET: 'StyleIndex',    // 文体テンプレ&例文
  BASELINE_SHEET: 'Baselines',       // しきい値

  TOP_CARDS_PER_CAMPAIGN: 3,         // キャンペーンごとのナレッジ件数（AIに渡す）

  // いまは search のみ → メタ固定（将来 multi-channel 化の時は null にして inferMeta_ を再有効化）
  FORCE_META: { channel: 'search', brand: 'non' },

  // 基準値のデフォルト（シートが無い場合のフェイルセーフ）
  DEFAULT_BASELINES: {
    CPA: { direction: 'lower_is_better', target: 15000, good_max: 15000, bad_min: 20000, min_impr: 100 },
    CTR: { direction: 'higher_is_better', target: 0.03,  good_max: 0.03,  bad_min: 0.02,  min_impr: 1000 },
    CVR: { direction: 'higher_is_better', target: 0.03,  good_max: 0.03,  bad_min: 0.02,  min_impr: 200 }
  }
};

// ===== オーケストレーション =====
function pipelineAndNotify() {
  const ssUrl = SpreadsheetApp.getActive().getUrl(); // このシート
  const { titleJP, bodyText } = buildWeeklySummary(ssUrl);
  postToSlack(titleJP, bodyText);
}

// ===== 週次要約の作成 =====
function buildWeeklySummary(SPREADSHEET_URL) {
  const ss = SpreadsheetApp.openByUrl(SPREADSHEET_URL);
  const ws = ss.getSheetByName(CFG.WEEKLY_SHEET);
  if (!ws) throw new Error('WeeklyAgg シートが見つかりません');

  const lastRow = ws.getLastRow();
  if (lastRow < 2) throw new Error('WeeklyAgg にデータがありません');

  const numCols = ws.getLastColumn();
  const values = ws.getRange(2, 1, lastRow - 1, numCols).getValues();

  const idx = { WeekStart: 0, WeekEnd: 1, Campaign: 2, Impr: 3, Clicks: 4, CV: 5, Cost: 6, CTR: 7, CVR: 8, CPC: 9, CPA: 10 };

  // 最新から直近N週の WeekStart を取得
  const weekStartsYMD = [];
  for (let i = values.length - 1; i >= 0; i--) {
    const ymd = normalizeYMD(values[i][idx.WeekStart]);
    if (!ymd) continue;
    if (weekStartsYMD.indexOf(ymd) === -1) {
      weekStartsYMD.push(ymd);
      if (weekStartsYMD.length >= CFG.LOOKBACK_WEEKS) break;
    }
  }
  weekStartsYMD.reverse();

  const targetRows = values.filter(r => weekStartsYMD.includes(normalizeYMD(r[idx.WeekStart])));
  if (targetRows.length === 0) throw new Error('対象週のデータが見つかりません');

  const latestStartYMD = weekStartsYMD[weekStartsYMD.length - 1];
  const prevStartYMD = (weekStartsYMD.length >= 2) ? weekStartsYMD[weekStartsYMD.length - 2] : null;

  const latestRows = targetRows.filter(r => normalizeYMD(r[idx.WeekStart]) === latestStartYMD);
  const prevRows   = prevStartYMD ? targetRows.filter(r => normalizeYMD(r[idx.WeekStart]) === prevStartYMD) : [];
  const latestEndYMD = normalizeYMD(latestRows[0][idx.WeekEnd]);

  const titleJP = buildJapaneseTitleFromYMD(latestStartYMD, latestEndYMD);

  // KPIテーブル（TSV; AIに渡す原表）
  const header = ['WeekStart', 'WeekEnd', 'Campaign', 'Impr', 'Clicks', 'CV', 'Cost', 'CTR', 'CVR', 'CPC', 'CPA'];
  const lines = [header.join('\t')];
  targetRows.forEach(r => {
    lines.push([
      normalizeYMD(r[idx.WeekStart]),
      normalizeYMD(r[idx.WeekEnd]),
      r[idx.Campaign],
      r[idx.Impr], r[idx.Clicks], r[idx.CV], r[idx.Cost],
      r[idx.CTR], r[idx.CVR], r[idx.CPC], r[idx.CPA]
    ].join('\t'));
  });
  const kpiTable = lines.join('\n');

  // === KPI結果（“固定”でSlack表示する行）をGASで生成 ===
  const { kpiLinesText, numericWhitelist } = buildKpiLinesAndWhitelist_(latestRows, prevRows, idx);

  // === 参照カードコンテキストの構築（最新週の各キャンペーン向け） ===
  const baselines = loadBaselinesV2_(ss); // キャンペーン別 + global
  const refContextPerCampaign = latestRows.map(r => {
    const name = String(r[idx.Campaign] || '');
    const issues = deriveIssuesForRow_(r, idx, baselines); // 例: ['CPA_UP','CTR_DOWN']
    const meta = CFG.FORCE_META || inferMeta_(name);       // 現状は {search, non} を使用
    const cards = pickKnowledgeCards_(ss, issues, meta, name, CFG.TOP_CARDS_PER_CAMPAIGN); // Doc+Report統合
    return { name, issues, meta, ref_cards: cards };
  });

  // 文体ガイド（StyleIndex）を抽出（exemplarはマスク）
  const styleMeta = CFG.FORCE_META || { channel:'search', brand:'non' };
  const styleGuide = pickStyleGuide_(ss, styleMeta, 2); // マスク済みexemplarsを返す

  // 追加：RawData 由来の数値も許可リストへ（任意）
  const rawWhitelist = buildRawWhitelist_(ss);
  const NUMERIC_WHITELIST = Array.from(new Set([].concat(numericWhitelist, rawWhitelist))).slice(0, 3000); // 念のため上限

  // === プロンプト ===（StyleIndexのガイドを厳守。例文は参照のみ＆コピー禁止）
  const prompt = `
あなたは日本語で、前向きで建設的なトーンを基本にレポートを書くアナリストです。
数字の推測・創作は禁止。**数値は必ず WeeklyAgg（下記TSV）または RawData（許可リストに含まれる値）由来のみ**を使用すること。

【文体ガイド（厳守）】
構成テンプレート：
${styleGuide.structure_template || `:white_check_mark: 進捗 :
 • <1〜3行のポジティブ要点>

:warning: 課題 :
 • <本当にクリティカルな点がある場合のみ>

:dash: ネクストアクション
 • <即実行×インパクト高い順に2〜4件>

:chart_with_upwards_trend: 結果
 • （このセクションは後段の『固定KPI』をそのまま出力すること）`}

表記ルール：${JSON.stringify(styleGuide.phrasing_rules || [
  "箇条書きは行頭に『 • 』（半角スペース+中黒）",
  "見出しは『:white_check_mark: 進捗 :』『:warning: 課題 :』『:dash: ネクストアクション』『:chart_with_upwards_trend: 結果』の4つのみ",
  "金額は¥+3桁カンマ、割合は%で小数2桁まで",
  "CV=0のCPAは—表記、前週比較は()内の『前週 : 』表記",
  "出力内で『\\n』は使わず実改行で段落化"
], null, 0)}

【この文体の“参考”（コピペ禁止・内容はダミー化済み）】
${(styleGuide.exemplars_masked||[]).map(e=>`(${e.style_id||'style'}/${e.recency||''})
${e.exemplar_text_masked||''}`).join('\n\n')}

【数値ルール（重要）】
- **下記『固定KPI』は、そのまま貼り付け（並び替え・改変禁止）**。
- それ以外のセクションで数値を記載する場合は、**『数値許可リスト』に含まれる値のみ**を使用。含まれない数値は記載しない（「増加/減少」「高/低」など非数値で表現）。

# 固定KPI（このまま出力すること）
${kpiLinesText}

# 数値許可リスト（上記KPIや表に含まれる数値のみ使用可）
${NUMERIC_WHITELIST.join(', ')}

# 参照カード（JSON; 各キャンペーンの課題カテゴリと知見カード）
${JSON.stringify({ campaigns: refContextPerCampaign }, null, 2)}

# データ（直近${CFG.LOOKBACK_WEEKS}週間、タブ区切りTSV。最新週を主に使う）
${kpiTable}
  `.trim();

  // === ★ 生成AIを1回だけ呼び出し（ここ） ===
  const raw = callOpenAI(prompt);
  // 改行整形 → 見出しエイリアス正規化 → セクション配下は「 • 」強制 → 記号統一 → 結果を固定KPIに置換
  const normalized = normalizeNL_(raw);
  const withHeadings = enforceSlackEmojiHeadings_(normalized);      // 🇯🇵別名も英語ショートコードに正規化
  const bulletsEnsured = ensureBulletsUnderSections_(withHeadings); // 進捗/課題/ネクスト配下は必ず「 • 」
  const bulletsUnified = enforceDotBullets_(bulletsEnsured);        // -, *, ・ などを「 • 」に統一
  const finalText = forceReplaceResultsSection_(bulletsUnified, kpiLinesText);
  return { titleJP, bodyText: finalText };
}

// ===== OpenAI呼び出し =====
function callOpenAI(prompt) {
  const apiKey = PropertiesService.getScriptProperties().getProperty('OPENAI_API_KEY');
  if (!apiKey) throw new Error('OPENAI_API_KEY が未設定です');

  const url = 'https://api.openai.com/v1/chat/completions';
  const payload = {
    model: CFG.OPENAI_MODEL,
    messages: [
      {
        role: 'system',
        content:
          'あなたは日本語のマーケアナリストです。' +
          '前向き・建設的なトーンを基本に、直近週を主役にして簡潔にレポートします。' +
          '出力は StyleIndex の構成テンプレ・表記ルールに準拠し、余計な見出しは追加しないこと。' +
          '例文は参照のみ。文言のコピーは禁止。' +
          '数値は WeeklyAgg/RawData 由来のみ、許可値以外は出力しないこと。'
      },
      { role: 'user', content: prompt }
    ],
    temperature: CFG.OPENAI_TEMPERATURE,
    max_tokens: CFG.OPENAI_MAX_TOKENS
  };

  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    headers: { Authorization: `Bearer ${apiKey}` }
  });

  const json = JSON.parse(res.getContentText());
  return json.choices[0].message.content.trim();
}

// ===== Slack通知 =====
function postToSlack(title, text) {
  const webhook = PropertiesService.getScriptProperties().getProperty('SLACK_WEBHOOK_URL');
  if (!webhook) throw new Error('SLACK_WEBHOOK_URL が未設定です');

  const blocks = [
    { type: 'header', text: { type: 'plain_text', text: title } },
    { type: 'section', text: { type: 'mrkdwn', text } }
  ];
  UrlFetchApp.fetch(webhook, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({ blocks })
  });
}

// ===== 日付ユーティリティ =====
function normalizeYMD(v) {
  const tz = Session.getScriptTimeZone() || 'Asia/Tokyo';
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v)) {
    return Utilities.formatDate(v, tz, 'yyyy-MM-dd');
  }
  if (typeof v === 'string' && v) {
    const s = v.replace(/\//g, '-');
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    const d = new Date(s);
    if (!isNaN(d)) return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
  }
  return '';
}
function buildJapaneseTitleFromYMD(startYMD, endYMD) {
  if (!startYMD || !endYMD) return '週次広告レポート 日付不明';
  const s = new Date(startYMD + 'T00:00:00+09:00');
  const e = new Date(endYMD + 'T00:00:00+09:00');
  const youbi = ['日', '月', '火', '水', '木', '金', '土'];
  const pad2 = n => ('0' + n).slice(-2);
  const sTxt = `${s.getFullYear()}.${pad2(s.getMonth() + 1)}.${pad2(s.getDate())}（${youbi[s.getDay()] }）`;
  const eTxt = `${pad2(e.getMonth() + 1)}.${pad2(e.getDate())}（${youbi[e.getDay()] }）`;
  return `週次広告レポート ${sTxt} ~ ${eTxt}`;
}

// ===== シート読込ヘルパー =====
function readSheetAsObjects_(ss, sheetName) {
  const sh = ss.getSheetByName(sheetName);
  if (!sh) return [];
  const vals = sh.getDataRange().getValues();
  if (vals.length < 2) return [];
  const header = vals[0].map(h => String(h).trim());
  return vals.slice(1).filter(r => r.join('') !== '').map(r => {
    const o = {};
    header.forEach((h, i) => o[h] = r[i]);
    return o;
  });
}

// ===== Baselines（campaign別 + global、%/¥/カンマ正規化） =====
function loadBaselinesV2_(ss){
  const rows = readSheetAsObjects_(ss, CFG.BASELINE_SHEET);
  const out = { global:{}, perCampaign:{} };
  if (!rows.length) { out.global = CFG.DEFAULT_BASELINES; return out; }

  const first = rows[0] || {};
  const nameKey = ('campaign_name' in first) ? 'campaign_name'
                : ('campagin_name' in first) ? 'campagin_name'
                : 'campaign_name';

  rows.forEach(r=>{
    const metric = String(r.metric||'').toUpperCase(); if(!metric) return;
    const dir = String(r.direction||'lower_is_better');

    const rule = {
      direction: dir,
      target: parseMetricNumber_(metric, r.target),
      good_max: parseMetricNumber_(metric, r.good_max),
      bad_min: parseMetricNumber_(metric, r.bad_min),
      min_impr: r.min_impr ? Number(String(r.min_impr).replace(/[, ]/g,'')) : 0,
      min_clicks: r.min_clicks ? Number(String(r.min_clicks).replace(/[, ]/g,'')) : 0,
      min_cv: r.min_cv ? Number(String(r.min_cv).replace(/[, ]/g,'')) : 0
    };

    const cname = (r[nameKey]||'').toString().trim();
    if (!cname || /^global$/i.test(cname)) {
      out.global[metric] = rule;
    } else {
      if (!out.perCampaign[cname]) out.perCampaign[cname] = {};
      out.perCampaign[cname][metric] = rule;
    }
  });

  out.global = Object.assign({}, CFG.DEFAULT_BASELINES, out.global);
  return out;
}

// 通貨/％/カンマを数値に正規化（CTR/CVRは%→小数へ）
function parseMetricNumber_(metric, v){
  if (v == null || v === '') return null;
  let s = (typeof v === 'number') ? String(v) : String(v).trim();
  const hasPercent = /%$/.test(s);
  s = s.replace(/[¥￥, ]/g,'').replace(/%$/,'');
  const num = parseFloat(s);
  if (isNaN(num)) return null;
  if (/(CTR|CVR)/i.test(metric)) return hasPercent ? num/100 : num; // 5.03% → 0.0503
  return num; // CPA/CPC/CVなどはそのまま
}

// キャンペーンに合う基準を取得（campaign優先 → global → デフォルト）
function getBaselineFor_(baselines, campaignName, metric){
  const pc = baselines.perCampaign[campaignName];
  if (pc && pc[metric]) return pc[metric];
  if (baselines.global && baselines.global[metric]) return baselines.global[metric];
  return CFG.DEFAULT_BASELINES[metric] || null;
}

// 単一KPIの良/警告/悪 判定
function judge_(value, bl, impr) {
  if (value == null || value === '' || isNaN(value)) return { status: 'NA' };
  if (impr != null && bl.min_impr && Number(impr) < bl.min_impr) return { status: 'INSUFFICIENT' };
  const v = Number(value);
  const good = Number(bl.good_max);
  const bad = Number(bl.bad_min);
  if (bl.direction === 'lower_is_better') {
    if (v <= good) return { status: 'GOOD' };
    if (v >= bad) return { status: 'BAD' };
    return { status: 'WARN' };
  } else {
    if (v >= good) return { status: 'GOOD' };
    if (v <= bad) return { status: 'BAD' };
    return { status: 'WARN' };
  }
}

// ===== 課題判定（CPA/CTR/CVR/CPC/CV → issue_category）=====
function deriveIssuesForRow_(row, idx, baselines) {
  const issues = [];
  const impr = Number(row[idx.Impr]);
  const name = String(row[idx.Campaign]||'');

  const METRICS = ['CPA','CTR','CVR','CPC','CV'];

  METRICS.forEach(m=>{
    const bl = getBaselineFor_(baselines, name, m);
    if (!bl) return;

    // 母数ガード（任意列がある場合のみ）
    if (bl.min_impr && Number(impr) < bl.min_impr) return;
    if (bl.min_clicks && Number(row[idx.Clicks]) < bl.min_clicks) return;
    if (bl.min_cv && Number(row[idx.CV]) < bl.min_cv) return;

    const j = judge_(row[idx[m]], bl, impr);
    if (j.status === 'BAD') {
      if (m==='CPA') issues.push('CPA_UP');
      if (m==='CTR') issues.push('CTR_DOWN');
      if (m==='CVR') issues.push('CVR_DOWN');
      if (m==='CPC') issues.push('CPC_UP');
      if (m==='CV')  issues.push('CV_DOWN');
    }
  });

  return Array.from(new Set(issues));
}

// ===== 簡易メタ推定（現状は未使用：将来 multi-channel 化時に再有効化）=====
function inferMeta_(campaignName) {
  return {
    channel: /Display|GDN|YouTube|Video/i.test(campaignName) ? 'display' : 'search',
    brand: /brand|指名|自社名/i.test(campaignName) ? 'brand' : 'non'
  };
}

// ===== ナレッジ抽出（DocIndex + ReportIndex を統合）=====
function pickKnowledgeCards_(ss, issues, meta, campaignName, topN) {
  const docRows = readSheetAsObjects_(ss, CFG.DOCINDEX_SHEET);
  const repRows = readSheetAsObjects_(ss, CFG.REPORTINDEX_SHEET);
  const rows = []
    .concat((docRows||[]).map(r => Object.assign({_source:'doc'}, r)))
    .concat((repRows||[]).map(r => Object.assign({_source:'report'}, r)));

  if (!rows.length || !issues.length) return [];

  const scoreRow = r => {
    if (!issues.includes(String(r.issue_category||'').trim())) return -1e9;

    // フィット（channel / brand）
    let fit = 0;
    try {
      const m = JSON.parse(r.campaign_meta || '{}');
      if (m.channel && meta.channel && m.channel === meta.channel) fit += 10;
      if (m.brand  && meta.brand  && m.brand  === meta.brand)  fit += 10;
    } catch(e){}

    // 同一キャンペーンは強化（ReportIndex想定）
    if (String(r.campaign_name||'').trim() && r.campaign_name === campaignName) fit += 15;

    // 成果ブースト（過去レポの効果量）
    let eff = 0;
    try {
      const ef = JSON.parse(r.outcome_effect || '{}');
      eff = Math.abs(ef.CPA||0)+Math.abs(ef.CV||0)+Math.abs(ef.CTR||0)+Math.abs(ef.CVR||0);
    } catch(e){}

    const q   = Number(r.quality_score || 50);
    const rec = daysAgo_(r.recency) < 60 ? 5 : 0;
    const src = (String(r._source||'doc') === 'report') ? 3 : 0;

    return fit + q + rec + (eff*30) + src;
  };

  return rows
    .map(r => Object.assign({}, r, { _score: scoreRow(r) }))
    .filter(r => r._score > -1e8)
    .sort((a,b)=> b._score - a._score)
    .slice(0, topN)
    .map(r => ({
      title: r.title,
      key_takeaways: r.key_takeaways,
      checklist: safeParseArray_(r.checklist),
      source: String(r._source||'doc'),
      recency: r.recency || ''
    }));
}

// ===== 文体ガイド抽出（StyleIndex：exemplarは中身をマスク）=====
function pickStyleGuide_(ss, meta, maxExamples){
  const rows = readSheetAsObjects_(ss, CFG.STYLEINDEX_SHEET) || [];
  if (!rows.length) return {
    structure_template: unescapeNL_(`:white_check_mark: 進捗 :
 • <1〜3行のポジティブ要点>

:warning: 課題 :
 • <本当にクリティカルな点がある場合のみ>

:dash: ネクストアクション
 • <即実行×インパクト高い順に2〜4件>

:chart_with_upwards_trend: 結果
 • 全体 : CPA¥<num>、CV<num>、Cost¥<num>
 • <キャンペーン> : CPA¥<num>、CV<num>、Cost¥<num>（前週 : CPA¥<num>、CV<num>、Cost¥<num>）`),
    phrasing_rules: [],
    exemplars_masked: []
  };

  const score = r => {
    let s = Number(r.priority || 50);
    if (String(r.channel||'') === meta.channel) s += 10;
    if (String(r.brand||'')   === meta.brand)   s += 10;
    if (daysAgo_(r.recency) < 60) s += 5;
    return s;
  };

  const sorted = rows.map(r => Object.assign(r, { _s: score(r) })).sort((a,b)=> b._s - a._s);
  const top = sorted[0] || {};
  const exemplars_masked = sorted.slice(0, maxExamples||2).map(r => ({
    style_id: r.style_id,
    recency: r.recency || '',
    exemplar_text_masked: maskExemplarForPrompt_(String(r.exemplar_text || ''))
  }));
  return {
    structure_template: unescapeNL_(String(top.structure_template || '')),
    phrasing_rules: safeParseArray_(top.phrasing_rules),
    exemplars_masked
  };
}

// ===== KPI（“結果”セクション）行の生成＋数値許可リスト =====
function buildKpiLinesAndWhitelist_(latestRows, prevRows, idx){
  // 全体集計（最新週）
  const totalCost = latestRows.reduce((s,r)=> s + Number(r[idx.Cost]||0), 0);
  const totalCV   = latestRows.reduce((s,r)=> s + Number(r[idx.CV]||0), 0);
  const totalCPA  = (totalCV>0) ? Math.round(totalCost/totalCV) : null;

  const lines = [];
  lines.push(':chart_with_upwards_trend: 結果');
  lines.push(` • 全体 : CPA${fmtYen(totalCPA)}、CV${fmtInt(totalCV)}、Cost${fmtYen(totalCost)}`);

  // 直近週の各キャンペーン
  const prevMap = {};
  prevRows.forEach(r => { prevMap[String(r[idx.Campaign]||'')] = r; });

  latestRows.forEach(r=>{
    const name = String(r[idx.Campaign]||'');
    const cost = Number(r[idx.Cost]||0);
    const cv   = Number(r[idx.CV]||0);
    const cpa  = (cv>0) ? Math.round(cost/cv) : null;

    const pr = prevMap[name];
    const prevCost = pr ? Number(pr[idx.Cost]||0) : null;
    const prevCV   = pr ? Number(pr[idx.CV]||0)   : null;
    const prevCPA  = (pr && prevCV>0) ? Math.round(prevCost/prevCV) : null;

    const prevTxt = pr ? `（前週 : CPA${fmtYen(prevCPA)}、CV${fmtInt(prevCV)}、Cost${fmtYen(prevCost)}）` : '';
    lines.push(` • ${name} : CPA${fmtYen(cpa)}、CV${fmtInt(cv)}、Cost${fmtYen(cost)}${prevTxt}`);
  });

  const kpiLinesText = lines.join('\n');

  // 数値許可リスト（KPI行に含まれる数値はすべて許可）
  const whitelist = extractNumericTokens_(kpiLinesText);
  return { kpiLinesText, numericWhitelist: whitelist };
}

// 任意：RawDataがあれば、当該週の数値を許可リストへ追加（フォーマット不問で数値だけ抽出）
function buildRawWhitelist_(ss){
  const sh = ss.getSheetByName(CFG.RAW_SHEET);
  if (!sh) return [];
  const vals = sh.getDataRange().getValues();
  if (!vals || vals.length<2) return [];
  // 2〜300行程度だけ軽くスキャン（過剰肥大回避）
  const maxScan = Math.min(vals.length-1, 300);
  let text = '';
  for (let i=1; i<=maxScan; i++){
    text += vals[i].join('\t') + '\n';
  }
  return extractNumericTokens_(text);
}

// “結果”セクションを固定KPIで強制置換
function forceReplaceResultsSection_(text, kpiLinesText){
  const lines = String(text||'').replace(/\r\n?/g,'\n').split('\n');
  const out = [];
  let i=0;
  while(i<lines.length){
    const t = lines[i];
    if (/^:chart_with_upwards_trend:\s*結果/.test(t.trim())){
      // 次の見出しまでをスキップし、固定KPIを挿入
      out.push(':chart_with_upwards_trend: 結果');
      out.push(...kpiLinesText.split('\n').slice(1)); // 先頭見出しは重複するので除外
      i++;
      while(i<lines.length && !isHeadingLine_(lines[i])) i++;
      continue; // 見出し行でループ継続（iは次の見出し位置）
    } else {
      out.push(t);
      i++;
    }
  }
  return out.join('\n');
}

// ===== 体裁ユーティリティ =====
function fmtInt(n){
  if (n==null || isNaN(n)) return '-';
  return Number(n).toLocaleString('ja-JP');
}
function fmtYen(n){
  if (n==null || isNaN(n)) return '—';
  return '¥' + Number(n).toLocaleString('ja-JP');
}

// 実改行に変換（入力用 / 出力保険）
function unescapeNL_(s){ return String(s||'').replace(/\\n/g, '\n'); }
function normalizeNL_(s){ return String(s||'').replace(/\r\n?/g, '\n').replace(/\\n/g, '\n'); }

// 見出しかどうか判定（Slackショートコード or 絵文字 or 「結果/実績」）
function isHeadingLine_(line){
  const t = String(line||'').trim();
  if (/^:(white_check_mark|warning|dash|chart_with_upwards_trend):\s*.*$/.test(t)) return true;
  if (/^(✅|⚠️|💨|📈)\s*.*$/.test(t)) return true;
  if (/^(結果|実績)\s*:?\s*$/.test(t)) return true;
  return false;
}

// 絵文字/日本語エイリアス見出し → Slackショートコード見出しへ正規化
function enforceSlackEmojiHeadings_(s){
  const lines = String(s||'').replace(/\r\n?/g, '\n').split('\n');
  for (let i=0; i<lines.length; i++){
    let t = lines[i].trim();

    // 🇯🇵→🇺🇸 エイリアス正規化（必要に応じて追加）
    t = t
      .replace(/:チェックマーク_緑:/g, ':white_check_mark:')
      .replace(/:警告:/g, ':warning:')
      .replace(/:ダッシュ:/g, ':dash:')
      .replace(/:上昇折れ線グラフ:/g, ':chart_with_upwards_trend:');

    // 絵文字 → Slackショートコード
    t = t.replace(/^✅\s*(進捗)\s*:?\s*$/,':white_check_mark: $1 :')
         .replace(/^⚠️\s*(課題)\s*:?\s*$/,':warning: $1 :')
         .replace(/^💨\s*(ネクストアクション)\s*:?\s*$/,':dash: $1')
         .replace(/^📈\s*(結果)\s*:?\s*$/,':chart_with_upwards_trend: $1');

    // 日本語見出し素 → 結果見出しの正規化
    if (/^(結果|実績)\s*:?\s*$/.test(t)) t = ':chart_with_upwards_trend: 結果';

    // 英語ショートコードの体裁を統一（末尾コロンなど）
    if (/^:white_check_mark:\s*進捗\s*:?\s*$/.test(t)) t = ':white_check_mark: 進捗 :';
    if (/^:warning:\s*課題\s*:?\s*$/.test(t)) t = ':warning: 課題 :';
    if (/^:dash:\s*ネクストアクション\s*:?\s*$/.test(t)) t = ':dash: ネクストアクション';
    if (/^:chart_with_upwards_trend:\s*結果\s*:?\s*$/.test(t)) t = ':chart_with_upwards_trend: 結果';

    // 見出し行の頭に付いた「•」などは除去
    t = t.replace(/^\s*•\s+/, '');
    lines[i] = t;
  }
  return lines.join('\n');
}

// 見出し配下は必ず「 • 」で始める（結果セクションは対象外：あとで固定KPIに置換するため）
function ensureBulletsUnderSections_(s){
  const lines = String(s||'').replace(/\r\n?/g,'\n').split('\n');

  const isHeading = l => /^:(white_check_mark|warning|dash|chart_with_upwards_trend):\s*(進捗|課題|ネクストアクション|結果)\s*:?\s*$/.test(String(l).trim())
                      || /^(✅|⚠️|💨|📈)\s*(進捗|課題|ネクストアクション|結果)\s*:?\s*$/.test(String(l).trim());

  let section = null; // 'progress' | 'issues' | 'actions' | 'results' | null

  for (let i=0;i<lines.length;i++){
    const raw = lines[i];
    const t = String(raw||'');
    const tt = t.trim();

    if (isHeading(tt)){
      if (/white_check_mark/.test(tt) || /^✅/.test(tt)) section = 'progress';
      else if (/warning/.test(tt) || /^⚠️/.test(tt))     section = 'issues';
      else if (/dash/.test(tt) || /^💨/.test(tt))         section = 'actions';
      else if (/chart_with_upwards_trend/.test(tt) || /^📈/.test(tt)) section = 'results';
      lines[i] = tt;
      continue;
    }

    // 「結果」は固定KPIに置換するので触らない
    if (section && section !== 'results'){
      if (tt && !/^\s*•\s+/.test(t)){
        lines[i] = ' • ' + tt;
      } else {
        lines[i] = t;
      }
    } else {
      lines[i] = t;
    }
  }
  return lines.join('\n');
}

// 代表的な箇条書き記号を「 • 」に統一（見出しは除外）
function enforceDotBullets_(s){
  const lines = String(s||'').replace(/\r\n?/g, '\n').split('\n');
  for (let i=0; i<lines.length; i++){
    let line = lines[i];
    if (isHeadingLine_(line)) {
      lines[i] = line.replace(/^\s*[-*•・●▪︎▶︎►※]\s+/, '').replace(/^\s*•\s+/, '');
      continue;
    }
    if (/^\s*[-*•・●▪︎▶︎►※]\s+/.test(line)) {
      lines[i] = line.replace(/^\s*[-*•・●▪︎▶︎►※]\s+/, ' • ');
      continue;
    }
  }
  return lines.join('\n');
}

// テキストから「¥金額」「整数」「少数」「%」を抽出し、原形トークンを返す
function extractNumericTokens_(text){
  const s = String(text||'');
  const tokens = [];
  const push = v => { if (v && tokens.indexOf(v)===-1) tokens.push(v); };
  // 金額（¥12,345 / ¥123）
  const yen = s.match(/¥[\d,]+/g) || [];
  yen.forEach(push);
  // パーセント（12.34% / 5%）
  const pct = s.match(/\d+(?:\.\d+)?%/g) || [];
  pct.forEach(push);
  // 純数（整数/少数）— 桁が長すぎるものは除外
  const nums = s.match(/\b\d+(?:\.\d+)?\b/g) || [];
  nums.forEach(n=>{
    if (n.length<=8) push(n);
  });
  return tokens;
}

// exemplarの本文をマスクして「形だけ」渡す（コピー禁止対策）
function maskExemplarForPrompt_(text){
  const lines = unescapeNL_(String(text||'')).split(/\r?\n/);
  return lines.map(line => {
    if (isHeadingLine_(line)) return line.trim();               // 見出しは保持
    if (/^\s*•\s+/.test(line)) return line.replace(/^(\s*•\s*).+$/, '$1<例>'); // 箇条書き本文は潰す
    if (/^\s*$/.test(line)) return '';                          // 空行
    return '<例>';                                              // その他は潰す
  }).join('\n').replace(/\n{3,}/g, '\n\n');
}

// ===== recency（日数差）ユーティリティ =====
function daysAgo_(v) {
  if (v == null || v === '') return 9999;
  let d = null;
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v)) {
    d = v;
  } else if (typeof v === 'number') {
    d = new Date(v);
  } else {
    d = new Date(String(v));
  }
  if (isNaN(d)) return 9999;
  return Math.floor((Date.now() - d.getTime()) / 86400000);
}

// ===== JSONセーフパーサ =====
function safeParseArray_(s) {
  // シートに ["a","b"] / '["a","b"]' / a;b / a｜b / 改行 など混在しても配列で返す
  if (s == null) return [];
  if (Array.isArray(s)) return s;
  let str = String(s).trim();
  if (!str) return [];
  // まず JSON を試す（単引用→二重引用の軽補正も）
  try {
    const jsonish = str
      .replace(/^\s*'/, '"').replace(/'\s*$/, '"')
      .replace(/'\s*,\s*'/g, '","')
      .replace(/,?\s*'([^']*)'\s*:/g, ',"$1":') // オブジェクト風のキーも軽補正
      .replace(/^(\s*)\[?([^]*)\]?$/, (m, p1, p2) => `[${p2}]`); // カンマ区切り裸を配列に
    const parsed = JSON.parse(jsonish);
    if (Array.isArray(parsed)) return parsed.map(v => String(v));
  } catch(e){ /* fallbackへ */ }
  // 区切り記号で分割
  return str.split(/\r?\n|[;｜|、 ，,]/).map(v => v.trim()).filter(Boolean);
}

function safeParseObj_(s) {
  if (!s) return {};
  if (typeof s === 'object') return s;
  try {
    return JSON.parse(String(s));
  } catch(e) {
    return {}; // 壊れていたら空オブジェクト
  }
}
