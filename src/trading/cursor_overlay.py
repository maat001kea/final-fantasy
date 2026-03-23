"""Visual cursor overlay for human-like browser automation."""

from __future__ import annotations

CURSOR_DOM_ID = "cd-inner-wrapper"
CURSOR_EL_KEY = "__q_41"
CURSOR_POS_KEY = "__p_17"
CURSOR_MOVE_FN = "_xh7"
CURSOR_ANIMATE_FN = "_xa2"
CURSOR_HIDE_FN = "_xi4"
CURSOR_SHOW_FN = "_xo6"
CURSOR_CLICK_FN = "_xc9"
STEALTH_READY_KEY = "__u_x82_"

# JavaScript to inject a visible cursor into the page
CURSOR_OVERLAY_JS = """
(function() {
    // Remove existing cursor if any
    const existingCursor = document.getElementById('cd-inner-wrapper');
    if (existingCursor) {
        existingCursor.remove();
    }
    
    // Create cursor element
    const cursor = document.createElement('div');
    cursor.id = 'cd-inner-wrapper';
    cursor.innerHTML = `
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M5.5 3.5L18.5 12.5L12 13.5L9.5 20.5L5.5 3.5Z" fill="#333333" stroke="#ffffff" stroke-width="1.5"/>
            <path d="M5.5 3.5L18.5 12.5L12 13.5L9.5 20.5L5.5 3.5Z" fill="none" stroke="#000000" stroke-width="0.5"/>
        </svg>
    `;
    cursor.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 24px;
        height: 24px;
        pointer-events: none;
        z-index: 2147483647;
        transform: translate(-2px, -2px);
        transition: transform 0.05s ease-out;
        filter: drop-shadow(1px 1px 1px rgba(0,0,0,0.3));
    `;
    document.body.appendChild(cursor);
    
    // Store cursor reference globally
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
            
            if (progress < 1) {
                requestAnimationFrame(animate);
            } else {
                if (callback) callback();
            }
        }
        
        requestAnimationFrame(animate);
    };
    
    // Hide/show cursor
    window._xi4 = function() {
        if (window.__q_41) {
            window.__q_41.style.display = 'none';
        }
    };
    
    window._xo6 = function() {
        if (window.__q_41) {
            window.__q_41.style.display = 'block';
        }
    };
    
    // Click effect
    window._xc9 = function() {
        if (!window.__q_41) return;
        
        const original = window.__q_41.style.transform;
        window.__q_41.style.transform = 'translate(-2px, -2px) scale(0.8)';
        setTimeout(() => {
            window.__q_41.style.transform = original;
        }, 100);
    };
})();
"""

# JavaScript for anti-detection
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

    const navProto = Object.getPrototypeOf(navigator);
    defineGetter(navProto, "webdriver", () => undefined);
    defineGetter(navProto, "platform", () => "MacIntel");
    defineGetter(navProto, "languages", () => ["da-DK", "da", "en-US", "en"]);
    defineGetter(navProto, "hardwareConcurrency", () => 8);
    defineGetter(navProto, "deviceMemory", () => 8);

    if (!window.chrome) defineValue(window, "chrome", {});
    ["runtime", "app", "csi", "loadTimes"].forEach(prop => {
        if (!window.chrome[prop]) defineValue(window.chrome, prop, (prop === "csi" || prop === "loadTimes") ? makeNative(() => ({}), prop) : {});
    });

    const mimeSpecs = [
        { type: "application/pdf", suffixes: "pdf", description: "Portable Document Format" },
        { type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format" }
    ];
    defineGetter(navProto, "mimeTypes", () => mimeSpecs);
    defineGetter(navProto, "plugins", () => [
        { name: "PDF Viewer", filename: "internal-pdf-viewer", description: "Portable Document Format" },
        { name: "Chrome PDF Viewer", filename: "internal-pdf-viewer", description: "Portable Document Format" }
    ]);

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

    try {
        window.cdc_adoQpoasnfa76pfcZLmcfl_Array = undefined;
        window.cdc_adoQpoasnfa76pfcZLmcfl_Promise = undefined;
        window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol = undefined;
    } catch (_) {}

    // --- Audio Fingerprint Protection (Obfuscated) ---
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
                    for (let i = 0; i < dest.length; i++) {
                        dest[i] = dest[i] + _n;
                    }
                }, "copyFromChannel");
                
                return _buf;
            }, "getChannelData");
        } catch (_) {}
    };

    _ap(window.AudioContext);
    _ap(window.OfflineAudioContext);
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
