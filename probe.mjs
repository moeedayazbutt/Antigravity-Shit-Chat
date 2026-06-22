import WebSocket from 'ws';

const WS_URL = 'ws://127.0.0.1:9000/devtools/page/D03B2F36B536AA7DCF8CEAA80EF9CE60';
const ws = new WebSocket(WS_URL);
let id = 1;

const call = (method, params) => new Promise((resolve) => {
    const msgId = id++;
    const handler = (msg) => {
        const data = JSON.parse(msg);
        if (data.id === msgId) { ws.off('message', handler); resolve(data.result); }
    };
    ws.on('message', handler);
    ws.send(JSON.stringify({ id: msgId, method, params }));
});

ws.on('open', async () => {
    await call('Runtime.enable', {});
    await new Promise(r => setTimeout(r, 800));

    // Find the best chat content element
    const result = await call('Runtime.evaluate', {
        expression: `JSON.stringify((() => {
            // Find all scrollable divs and rank them
            const candidates = Array.from(document.querySelectorAll('div')).map(d => {
                const s = window.getComputedStyle(d);
                const isScrollable = s.overflowY === 'auto' || s.overflowY === 'scroll';
                const ratio = d.scrollHeight / Math.max(d.clientHeight, 1);
                return {
                    isScrollable,
                    scrollH: d.scrollHeight,
                    clientH: d.clientHeight,
                    ratio,
                    className: d.className.slice(0, 100),
                    id: d.id,
                    hasScrollbarHide: d.classList.contains('scrollbar-hide'),
                    childCount: d.childElementCount,
                    innerTextLen: d.innerText?.length || 0
                };
            }).filter(d => d.isScrollable && d.scrollH > 500 && d.ratio > 1.2);
            
            // Sort by content richness
            candidates.sort((a,b) => b.innerTextLen - a.innerTextLen);
            return candidates.slice(0, 5);
        })())`,
        returnByValue: true
    });

    console.log('=== BEST CHAT CANDIDATES ===');
    const val = JSON.parse(result?.result?.value || '[]');
    val.forEach((c, i) => console.log(`[${i}]`, JSON.stringify(c, null, 2)));

    // Also get a small HTML sample from the top candidate
    const htmlResult = await call('Runtime.evaluate', {
        expression: `JSON.stringify((() => {
            const el = Array.from(document.querySelectorAll('div'))
                .filter(d => {
                    const s = window.getComputedStyle(d);
                    return (s.overflowY === 'auto' || s.overflowY === 'scroll') && d.scrollHeight > 500 && d.scrollHeight / d.clientHeight > 1.2;
                })
                .sort((a,b) => (b.innerText?.length||0) - (a.innerText?.length||0))[0];
            if (!el) return {found: false};
            return {
                found: true,
                className: el.className.slice(0,150),
                id: el.id,
                textSample: el.innerText?.slice(0, 200),
                htmlLen: el.outerHTML.length
            };
        })())`,
        returnByValue: true
    });
    console.log('\n=== TOP CANDIDATE HTML SAMPLE ===');
    console.log(JSON.parse(htmlResult?.result?.value || '{}'));

    ws.close();
    process.exit(0);
});

ws.on('error', (e) => { console.error('WS error:', e.message); process.exit(1); });
setTimeout(() => { console.log('timeout'); process.exit(1); }, 10000);
