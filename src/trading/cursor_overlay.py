"""Visual cursor overlay for human-like browser automation with max stealth."""

from __future__ import annotations

CURSOR_DOM_ID = "__tradovate_ui_helper"  # Skjult, uskyldigt ID
CURSOR_EL_KEY = "__q_41"
CURSOR_POS_KEY = "__p_17"
CURSOR_MOVE_FN = "_xh7"
CURSOR_ANIMATE_FN = "_xa2"
CURSOR_HIDE_FN = "_xi4"
CURSOR_SHOW_FN = "_xo6"
CURSOR_CLICK_FN = "_xc9"
STEALTH_READY_KEY = "__u_x82_"

# JavaScript to inject a visible cursor into the page using a Closed Shadow DOM
CURSOR_OVERLAY_JS = """
(function() {
    // Fjern eksisterende host hvis den findes
    const existingHost = document.getElementById('__tradovate_ui_helper');
    if (existingHost) {
        existingHost.remove();
    }
    
    // Opret en uskyldig "host" div
    const host = document.createElement('div');
    host.id = '__tradovate_ui_helper';
    host.style.cssText = 'position: fixed; top: 0; left: 0; width: 0; height: 0; z-index: 2147483647; pointer-events: none;';
    document.body.appendChild(host);
    
    // Opret en CLOSED Shadow DOM (skjuler SVG'en for Tradovate's egne scripts)
    const shadow = host.attachShadow({ mode: 'closed' });
    
    // Opret selve cursoren
    const cursor = document.createElement('div');
    cursor.innerHTML = `
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M5.5 3.5L18.5 12.5L12 13.5L9.5 20.5L5.5 3.5Z" fill="#333333" stroke="#ffffff" stroke-width="1.5"/>
            <path d="M5.5 3.5L18.5 12.5L12 13.5L9.5 20.5L5.5 3.5Z" fill="none" stroke="#000000" stroke-width="0.5"/>
        </svg>
    `;
    cursor.style.cssText = `
        position: absolute;
        top: 0;
        left: 0;
        width: 24px;
        height: 24px;
        pointer-events: none;
        transform: translate(-2px, -2px);
        transition: transform 0.05s ease-out;
        filter: drop-shadow(1px 1px 1px rgba(0,0,0,0.3));
    `;
    
    // Tilføj cursoren til skyggen
    shadow.appendChild(cursor);
    
    // Gem referencer globalt så cdp_adapter.py kan fjernstyre den
    window.__q_41 = cursor;
    window.__p_17 = { x: 0, y: 0 };
    
    // Move cursor function
    window._xh7 = function(x, y) {
        if (window.__q_41) {
            window.__q_41.style.left = x + 'px';
            window.__q_41.style.top = y + 'px';
            window.__p_17 = { x: x, y: y };
        }
    };
    
    // Animate cursor along path
    window._xa2 = function(path, durationMs, callback) {
        if (!window.__q_41 || !path || path.length === 0) {
            if (callback) callback();
            return;
        }
        const startTime = performance.now();
        const totalPoints = path.length;
        function animate(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / durationMs, 1);
            const index = Math.min(Math.floor(progress * totalPoints), totalPoints - 1);
            const point = path[index];
            window.__q_41.style.left = point.x + 'px';
            window.__q_41.style.top = point.y + 'px';
            window.__p_17 = { x: point.x, y: point.y };
            
            if (progress < 1) { requestAnimationFrame(animate); } 
            else { if (callback) callback(); }
        }
        requestAnimationFrame(animate);
    };
    
    window._xi4 = function() { if (window.__q_41) window.__q_41.style.display = 'none'; };
    window._xo6 = function() { if (window.__q_41) window.__q_41.style.display = 'block'; };
    
    // Click effect
    window._xc9 = function() {
        if (!window.__q_41) return;
        const original = window.__q_41.style.transform;
        window.__q_41.style.transform = 'translate(-2px, -2px) scale(0.8)';
        setTimeout(() => { window.__q_41.style.transform = original; }, 100);
    };
})();
"""

# JavaScript for anti-detection including Canvas, WebGL, and Audio spoofing
ANTI_DETECTION_JS = """
(function () {
    if (window.__u_x82_) return;
    Object.defineProperty(window, "__u_x82_", { value: true });

    const defineValue = (obj, prop, value) => {
        try { Object.defineProperty(obj, prop, { value, configurable: true, writable: false }); } catch (_) {}
    };
    const defineGetter = (obj, prop, getter) => {
        try { Object.defineProperty(obj, prop, { get: getter, configurable: true }); } catch (_) {}
    };
    const makeNative = (fn, name) => {
        defineValue(fn, "name", name);
        defineValue(fn, "toString", () => `function ${name}() { [native code] }`);
        return fn;
    };
// --- OS/Browser Alignment (Mac/Apple Profile) ---
    const navProto = Object.getPrototypeOf(navigator);
    defineGetter(navProto, "webdriver", () => undefined);
    defineGetter(navProto, "platform", () => "MacIntel");
    defineGetter(navProto, "languages", () => ["da-DK", "da", "en-US", "en"]);
    defineGetter(navProto, "hardwareConcurrency", () => 8);
    defineGetter(navProto, "deviceMemory", () => 8);
    
    // --- NYT: User-Agent og Vendor der matcher Mac-profilen ---
    defineGetter(navProto, "userAgent", () => "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36");
    defineGetter(navProto, "vendor", () => "Google Inc.");
    defineGetter(navProto, "appVersion", () => "5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36");

    if (!window.chrome) defineValue(window, "chrome", {});
    ["runtime", "app", "csi", "loadTimes"].forEach(prop => {
        if (!window.chrome[prop]) defineValue(window.chrome, prop, (prop === "csi" || prop === "loadTimes") ? makeNative(() => ({}), prop) : {});
    });

    // --- WebGL Fingerprint Protection ---
    const patchWebGL = (Ctor) => {
        if (!Ctor || !Ctor.prototype) return;
        const orig = Ctor.prototype.getParameter;
        defineValue(Ctor.prototype, "getParameter", makeNative(function(param) {
            if (param === 37445) return "Google Inc. (Apple)";
            if (param === 37446) return "ANGLE (Apple, ANGLE Metal Renderer: Apple GPU, Unspecified Version)";
            return orig.apply(this, arguments);
        }, "getParameter"));
    };
    patchWebGL(window.WebGLRenderingContext);
    patchWebGL(window.WebGL2RenderingContext);

    // --- ChromeDriver Artifact Removal ---
    try {
        window.cdc_adoQpoasnfa76pfcZLmcfl_Array = undefined;
        window.cdc_adoQpoasnfa76pfcZLmcfl_Promise = undefined;
        window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol = undefined;
    } catch (_) {}

    // --- Audio Fingerprint Protection ---
    const _ap = (Ctor) => {
        if (!Ctor || !Ctor.prototype) return;
        const _origGCD = Ctor.prototype.getChannelData;
        try {
            Ctor.prototype.getChannelData = makeNative(function getChannelData() {
                const _buf = _origGCD.apply(this, arguments);
                const _origCFC = _buf.copyFromChannel;
                _buf.copyFromChannel = makeNative(function(dest, chan, start) {
                    _origCFC.apply(this, arguments);
                    const _n = Math.random() * 0.0000001; 
                    for (let i = 0; i < dest.length; i++) { dest[i] = dest[i] + _n; }
                }, "copyFromChannel");
                return _buf;
            }, "getChannelData");
        } catch (_) {}
    };
    _ap(window.AudioContext);
    _ap(window.OfflineAudioContext);

    // --- HTML5 Canvas Fingerprint Protection ---
    const patchCanvas = () => {
        try {
            const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
            const origGetImageData = CanvasRenderingContext2D.prototype.getImageData;

            // Inject 5% noise into toDataURL
            defineValue(HTMLCanvasElement.prototype, "toDataURL", makeNative(function() {
                try {
                    const ctx = this.getContext('2d');
                    if (ctx && this.width > 0 && this.height > 0) {
                        const imageData = origGetImageData.call(ctx, 0, 0, this.width, this.height);
                        for (let i = 0; i < imageData.data.length; i += 4) {
                            if (Math.random() < 0.05) { 
                                imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + (Math.random() > 0.5 ? 1 : -1))); // Red
                            }
                        }
                        ctx.putImageData(imageData, 0, 0);
                    }
                } catch(e) {}
                return origToDataURL.apply(this, arguments);
            }, "toDataURL"));

            // Inject 5% noise into getImageData
            defineValue(CanvasRenderingContext2D.prototype, "getImageData", makeNative(function() {
                const imageData = origGetImageData.apply(this, arguments);
                try {
                    for (let i = 0; i < imageData.data.length; i += 4) {
                        if (Math.random() < 0.05) {
                            imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + (Math.random() > 0.5 ? 1 : -1))); // Red
                        }
                    }
                } catch(e) {}
                return imageData;
            }, "getImageData"));
        } catch (_) {}
    };
    patchCanvas();
})();
"""

def get_cursor_overlay_js() -> str:
    """Return JavaScript for cursor overlay injection."""
    return CURSOR_OVERLAY_JS

def get_anti_detection_js() -> str:
    """Return JavaScript for anti-detection."""
    return ANTI_DETECTION_JS

def get_full_injection_js() -> str:
    """Return combined JavaScript for full injection."""
    return ANTI_DETECTION_JS + "\n\n" + CURSOR_OVERLAY_JS