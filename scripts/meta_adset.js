// scripts/meta_adset.js

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

const META_TOKEN = process.env.META_TOKEN;
const META_AD_ACCOUNT = process.env.META_AD_ACCOUNT;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

console.log('🔧 환경변수 체크:');
console.log('META_TOKEN:', META_TOKEN ? '✅ 설정됨' : '❌ 없음');
console.log('META_AD_ACCOUNT:', META_AD_ACCOUNT ? '✅ 설정됨' : '❌ 없음');
console.log('SUPABASE_URL:', SUPABASE_URL ? '✅ 설정됨' : '❌ 없음');
console.log('SUPABASE_KEY:', SUPABASE_KEY ? '✅ 설정됨' : '❌ 없음');

console.log('🔍 환경변수 값 확인:');
console.log('META_TOKEN 길이:', META_TOKEN ? META_TOKEN.length : 0);
console.log('META_AD_ACCOUNT 값:', META_AD_ACCOUNT || '(없음)');
console.log('SUPABASE_URL 값:', SUPABASE_URL || '(없음)');
console.log('SUPABASE_KEY 길이:', SUPABASE_KEY ? SUPABASE_KEY.length : 0);

if (!META_TOKEN || !META_AD_ACCOUNT || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ 필수 환경변수가 누락되었습니다.');
  process.exit(1);
}

const supa = createClient(SUPABASE_URL, SUPABASE_KEY);

const getKSTYesterday = () => {
  const now = new Date();
  const kstOffset = 9 * 60 * 60 * 1000;
  const kstNow = new Date(now.getTime() + kstOffset);
  const kstYesterday = new Date(kstNow.getTime() - 24 * 60 * 60 * 1000);
  return kstYesterday.toISOString().slice(0, 10);
};

// 0) 테스트용 날짜 설정 (비워두면 어제 날짜로 작동)
const testDates = [];

const getTargetDate = () => {
  const envDate = process.env.TARGET_DATE;
  if (envDate && /^\d{4}-\d{2}-\d{2}$/.test(envDate)) {
    return envDate;
  }
  return null;
};

const toNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fetchWithRetry = async (url, { label = 'Meta API', maxRetries = 3 } = {}) => {
  const baseDelay = 30000;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url);

      if (res.ok) {
        return res.json();
      }

      const errorText = await res.text();
      let errorData;
      try {
        errorData = JSON.parse(errorText);
      } catch {
        errorData = { error: { message: errorText } };
      }

      const errorCode = errorData?.error?.code;
      const isTransient = errorData?.error?.is_transient === true;
      const isRateLimit = [4, 17, 32, 613].includes(errorCode) || isTransient;

      if (isRateLimit && attempt < maxRetries) {
        const waitTime = baseDelay * Math.pow(2, attempt - 1);
        console.log(`⚠️ ${label} Rate Limit (시도 ${attempt}/${maxRetries}): ${waitTime / 1000}초 대기 후 재시도...`);
        console.log(`📄 에러 내용: ${errorData?.error?.error_user_msg || errorData?.error?.message}`);
        await sleep(waitTime);
        continue;
      }

      console.error(`❌ ${label} 에러:`, res.status, res.statusText);
      console.error('응답 내용:', errorText);
      throw new Error(`${label} error: ${res.status} ${res.statusText}`);
    } catch (error) {
      if (attempt === maxRetries) {
        throw error;
      }
      console.log(`⚠️ ${label} 네트워크 에러 (시도 ${attempt}/${maxRetries}): 30초 후 재시도...`);
      await sleep(30000);
    }
  }

  throw new Error(`${label} 요청 실패`);
};

const chunk = (arr, size) => {
  const result = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
};

const fetchAdsetDetails = async (adsetIds) => {
  const details = {};
  const idChunks = chunk(adsetIds, 50);

  for (const idChunk of idChunks) {
    const detailsUrl = new URL('https://graph.facebook.com/v16.0/');
    detailsUrl.searchParams.set('ids', idChunk.join(','));
    detailsUrl.searchParams.set(
      'fields',
      [
        'id',
        'name',
        'daily_budget',
        'bid_strategy',
        'optimization_goal',
        'configured_status',
        'effective_status',
        'status',
        'learning_stage_info',
      ].join(',')
    );
    detailsUrl.searchParams.set('access_token', META_TOKEN);

    const data = await fetchWithRetry(detailsUrl.toString(), { label: 'Meta Adset Detail API' });
    Object.assign(details, data);
  }

  return details;
};

const fetchAccountTimezone = async () => {
  const url = new URL(`https://graph.facebook.com/v16.0/${META_AD_ACCOUNT}`);
  url.searchParams.set('fields', 'timezone_name');
  url.searchParams.set('access_token', META_TOKEN);

  try {
    const data = await fetchWithRetry(url.toString(), { label: 'Meta Ad Account API' });
    const timezone = data?.timezone_name;
    if (typeof timezone === 'string' && timezone.length > 0) {
      return timezone;
    }
  } catch (error) {
    console.error('⚠️ Meta 계정 타임존 조회 실패:', error.message);
  }

  return null;
};

const extractActionValue = (actions = [], type) => {
  const entry = actions.find((action) => action?.action_type === type);
  return toNumber(entry?.value);
};

const extractCostPerAction = (costPerActions = [], type) => {
  const entry = costPerActions.find((action) => action?.action_type === type);
  return toNumber(entry?.value);
};

async function fetchAndUpsertMetaAdset() {
  const envTargetDate = getTargetDate();
  const datesToRun = envTargetDate
    ? [envTargetDate]
    : Array.isArray(testDates) && testDates.length > 0
    ? testDates
    : [getKSTYesterday()];

  console.log(`\n📅 Meta 광고 세트 데이터 수집 시작 (총 ${datesToRun.length}개 날짜)`);

  const accountTimezone = await fetchAccountTimezone();

  for (const targetDate of datesToRun) {
    console.log(`\n📅 처리 날짜: ${targetDate}`);

    const baseUrl = new URL(`https://graph.facebook.com/v16.0/${META_AD_ACCOUNT}/insights`);
    baseUrl.searchParams.set('level', 'adset');
    baseUrl.searchParams.set('time_range', JSON.stringify({ since: targetDate, until: targetDate }));
    baseUrl.searchParams.set(
      'fields',
      [
        'date_start',
        'date_stop',
        'campaign_name',
        'adset_name',
        'adset_id',
        'impressions',
        'reach',
        'clicks',
        'ctr',
        'cpc',
        'spend',
        'cpm',
        'frequency',
        'actions',
        'action_values',
        'cost_per_action_type',
        'cost_per_result',
      ].join(',')
    );
    baseUrl.searchParams.set('access_token', META_TOKEN);
    baseUrl.searchParams.set('limit', '500');

    let pageUrl = baseUrl.toString();
    const insightRows = [];

    while (pageUrl) {
      console.log('🌐 Meta Adset Insights API 호출 중...');
      const responseData = await fetchWithRetry(pageUrl, { label: 'Meta Adset Insights API' });

      const pageData = Array.isArray(responseData?.data) ? responseData.data : [];
      insightRows.push(...pageData);

      pageUrl = responseData?.paging?.next || null;
      if (pageUrl) {
        console.log('🔁 다음 페이지 데이터가 존재합니다. 이어서 호출합니다.');
      }
    }

    console.log(`📊 Meta Adset Insights 수집 결과: ${insightRows.length}건`);

    if (insightRows.length === 0) {
      console.log('⚠️ 수집된 데이터가 없어 Supabase 저장을 건너뜁니다.');
      continue;
    }

    const adsetIds = Array.from(
      new Set(
        insightRows
          .map((row) => row?.adset_id)
          .filter((id) => typeof id === 'string' && id.length > 0)
      )
    );

    console.log(`🔎 광고 세트 상세 정보 조회 (총 ${adsetIds.length}개 ID)`);
    const adsetDetails = adsetIds.length > 0 ? await fetchAdsetDetails(adsetIds) : {};

    const rows = insightRows.map((row) => {
      const {
        date_start,
        date_stop,
        campaign_name,
        adset_name,
        adset_id,
        impressions,
        reach,
        clicks,
        ctr,
        cpc,
        spend,
        cpm,
        frequency,
        actions,
        cost_per_action_type,
        cost_per_result,
      } = row;

      const detail = adsetDetails[adset_id] || {};

      const landingPageViews = extractActionValue(actions, 'landing_page_view');
      const costPerLandingPageView = extractCostPerAction(cost_per_action_type || [], 'landing_page_view');
      const viewContent = extractActionValue(actions, 'view_content');
      const addToCart = extractActionValue(actions, 'add_to_cart');
      const purchases = extractActionValue(actions, 'purchase');

      const costPerResultRaw = toNumber(cost_per_result);
      const optimizedActionType = detail?.optimization_goal;

      const costCandidateTypes = [];
      if (typeof optimizedActionType === 'string' && optimizedActionType.length > 0) {
        costCandidateTypes.push(optimizedActionType);
        costCandidateTypes.push(optimizedActionType.toLowerCase());
        costCandidateTypes.push(optimizedActionType.toUpperCase());
      }
      costCandidateTypes.push(
        'purchase',
        'offsite_conversion.fb_pixel_purchase',
        'landing_page_view',
        'link_click',
        'view_content',
        'add_to_cart'
      );

      const costFromActions = costCandidateTypes.reduce((acc, type) => {
        if (acc > 0) {
          return acc;
        }
        return extractCostPerAction(cost_per_action_type || [], type);
      }, 0);

      const finalCostPerResult = costPerResultRaw > 0 ? costPerResultRaw : costFromActions;

      const dailyBudgetRaw = toNumber(detail?.daily_budget);
      const dailyBudget =
        dailyBudgetRaw > 0 ? dailyBudgetRaw / 100 : 0; // Meta budget 값은 통화의 최소 단위 기준

      const learningStageInfo = detail?.learning_stage_info;
      const learningPhase =
        learningStageInfo?.status ||
        learningStageInfo?.stage ||
        learningStageInfo?.description ||
        detail?.learning_phase ||
        null;

      return {
        date_start: date_start || targetDate,
        date_stop: date_stop || targetDate,
        time_zone: accountTimezone || null,
        campaign_name: campaign_name || null,
        adset_name: adset_name || null,
        adset_id: adset_id || null,
        impressions: toNumber(impressions),
        reach: toNumber(reach),
        clicks: toNumber(clicks),
        ctr: toNumber(ctr),
        cpc: toNumber(cpc),
        landing_page_views: landingPageViews,
        cost_per_landing_page_view:
          costPerLandingPageView > 0
            ? costPerLandingPageView
            : landingPageViews > 0
            ? toNumber(spend) / landingPageViews
            : 0,
        spend: toNumber(spend),
        cpm: toNumber(cpm),
        frequency: toNumber(frequency),
        view_content: viewContent,
        add_to_cart: addToCart,
        purchase: purchases,
        cost_per_result: finalCostPerResult,
        learning_phase: learningPhase,
        optimization_goal: detail?.optimization_goal || null,
        daily_budget: dailyBudget,
        bid_strategy: detail?.bid_strategy || null,
        status: detail?.status || detail?.effective_status || detail?.configured_status || null,
      };
    });

    const now = new Date().toISOString();
    rows.forEach((row) => {
      row.updated_at = now;
    });

    console.log('📝 저장 준비 데이터:', rows);

    console.log('💾 Supabase에 데이터 저장 중...');
    const { data: upsertData, error } = await supa
      .from('meta_adset_sm')
      .upsert(rows, { onConflict: ['date_start', 'adset_id'] });

    if (error) {
      console.error('❌ Supabase 에러:', error);
      throw error;
    }

    console.log('💾 Supabase 응답:', upsertData);
    console.log(`✅ ${targetDate} 광고 세트 데이터 ${rows.length}건 upsert 완료`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  fetchAndUpsertMetaAdset().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

export { fetchAndUpsertMetaAdset as fetchMetaAdsetData };
