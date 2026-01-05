import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';

export default {
  // 1. 定时触发 (Cloudflare Cron Triggers)
  async scheduled(event, env, ctx) {
    console.log("定时任务启动...");
    ctx.waitUntil(startWorkflow(env));
  },

  // 2. 网页访问触发 (方便测试)
  async fetch(request, env, ctx) {
    await startWorkflow(env);
    return new Response("任务已触发！请在 Cloudflare 后台实时日志查看进度。");
  }
};

async function startWorkflow(env) {
  const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_Secret_key);
  const gemini = new OpenAI({
    baseURL: 'https://generativelanguage.googleapis.com/v1beta/openai/',
    apiKey: env.GEMINI_API_KEY
  });

  try {
    const res = await fetch('https://zhibo.sina.com.cn/api/zhibo/feed?zhibo_id=152&page_size=60');
    const data = await res.json();
    const newsList = data.result.data.feed.list;

    // 1. 批量去重
    const allIds = newsList.map(item => item.id);
    const { data: existingRecords } = await supabase
      .from('earth724')
      .select('id')
      .in('id', allIds);

    const existingIdSet = new Set(existingRecords?.map(r => r.id) || []);
    const itemsToProcess = newsList.filter(item => !existingIdSet.has(item.id));

    if (itemsToProcess.length === 0) {
      console.log("没有新数据。");
      return;
    }

    // --- 核心改动：全量并发处理 ---
    console.log(`开始并发处理 ${itemsToProcess.length} 条新消息...`);
    
    const results = await Promise.all(
      itemsToProcess.map(async (item) => {
        try {
          const cleanText = item.rich_text.replace(/<[^>]+>/g, '');
          const geo = await getGeoInfo(gemini, cleanText);
          
          return {
            id: item.id,
            rich_text: item.rich_text,
            create_time: item.create_time,
            address: geo.address,
            latitude: geo.lat,
            longitude: geo.lng
          };
        } catch (err) {
          // 报错就跳过，返回 null
          console.warn(`[跳过] ID ${item.id} 异常: ${err.message}`);
          return null;
        }
      })
    );

    // 2. 过滤掉 null（失败的项）并批量入库
    const pendingData = results.filter(r => r !== null);

    if (pendingData.length > 0) {
      const { error } = await supabase.from('earth724').insert(pendingData);
      if (error) throw error;
      console.log(`[成功] 批量入库 ${pendingData.length} 条数据`);
    }

  } catch (err) {
    console.error("Workflow 异常:", err.message);
  }
}

async function getGeoInfo(ai, text) {
  const completion = await ai.chat.completions.create({
  model: "gemini-2.0-flash",
  messages: [
    { role: "system", content: `你是一个专业的地理空间情报专家。你的任务是从新闻文本中提取最核心的发生地点，并转化为经纬度坐标。
      规则如下：
      1. **定位优先级**：具体建筑/街道 > 具体公司名称 > 城市 > 国家。
      2. **多地关联处理**：如果涉及多国外交或冲突，请定位到“新闻主体机构”所在地或“事件第一发生现场”。
      3. **坐标标准**：必须返回 WGS84 坐标系的经纬度(lat, lng)。
      4. **语言要求**: address 字段请尽量保留原文中的地名表述，或翻译为清晰的中文。
      5. **格式约束**：必须严格返回 JSON, 不得包含任何 Markdown 格式块或多余解释。

      ...
      无法确定时，请按以下逻辑追溯：
      1. 金融行情/合约：定位至该品种对应的“主要交易所”总部（如：国际铜/沪金 -> 上海；布伦特原油 -> 伦敦；美股 -> 纽约）。
      2. 政策/政令：定位至发布该政策的最高行政机关所在地。
      3. 企业动态：定位至该企业的全球或区域总部。

      若根据上述逻辑仍完全无法推断（如：纯理论探讨），才返回 {"address": "未知", "lat": 0, "lng": 0}。` },

    { role: "user", 
      content: `请分析这段新闻。先思考：谁在说话？在哪里说话？涉及什么具体地点？然后输出 JSON: \n${text}`
    }
  ],
  response_format: { type: 'json_object' },
  temperature: 1.0
  });
  return JSON.parse(completion.choices[0].message.content);

}