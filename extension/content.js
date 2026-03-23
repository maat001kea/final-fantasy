/**
 * Privacy Shield — Content Script (runs in MAIN world at document_start)
 *
 * Defends against Canvas, WebGL, and AudioContext fingerprinting while
 * preserving normal website functionality. Spoofs OS/hardware signals
 * to align with a Mac profile and injects a status widget in a closed
 * Shadow DOM.
 */

(function privacyShield() {
  "use strict";

  // ─── Configuration ───────────────────────────────────────────────
  const MAC_PROFILE = {
    platform: "MacIntel",
    userAgent: navigator.userAgent
      .replace(/Windows NT \d+\.\d+/g, "Macintosh; Intel Mac OS X 14_4")
      .replace(/Linux x86_64/g, "Macintosh; Intel Mac OS X 14_4")
      .replace(/CrOS \w+ [\d.]+/g, "Macintosh; Intel Mac OS X 14_4"),
    vendor: "Google Inc.",
    webglVendor: "Google Inc. (Apple)",
    webglRenderer: "ANGLE (Apple, Apple M2, OpenGL 4.1)",
  };

  // Deterministic per-session seed so noise is consistent within a page
  // load but different across sessions.
  const SESSION_SEED = Math.floor(Math.random() * 0xffffffff);

  // ─── Utility: simple seeded PRNG (mulberry32) ────────────────────
  function mulberry32(seed) {
    return function () {
      seed |= 0;
      seed = (seed + 0x6d2b79f5) | 0;
      let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 0xffffffff;
    };
  }

  // ─────────────────────────────────────────────────────────────────
  // 1. Canvas Fingerprinting Defense
  // ─────────────────────────────────────────────────────────────────

  const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
  const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;

  /**
   * Applies microscopic noise to pixel data. Changes are ±1 on roughly
   * 1 in 10 color channels — invisible to the human eye but enough to
   * defeat fingerprint hashing.
   */
  function perturbPixels(imageData, canvasId) {
    const data = imageData.data;
    const rng = mulberry32(SESSION_SEED ^ canvasId);

    for (let i = 0; i < data.length; i += 4) {
      // Only touch ~10% of pixels to minimise visual impact
      if (rng() > 0.1) continue;

      for (let ch = 0; ch < 3; ch++) {
        // Skip alpha channel entirely to avoid transparency artefacts
        const val = data[i + ch];
        // Apply ±1 noise, clamped to [0, 255]
        const delta = rng() < 0.5 ? -1 : 1;
        data[i + ch] = Math.max(0, Math.min(255, val + delta));
      }
    }
    return imageData;
  }

  // Per-canvas monotonic ID for deterministic-but-unique noise
  const canvasIdMap = new WeakMap();
  let nextCanvasId = 0;

  function getCanvasId(canvas) {
    let id = canvasIdMap.get(canvas);
    if (id === undefined) {
      id = nextCanvasId++;
      canvasIdMap.set(canvas, id);
    }
    return id;
  }

  HTMLCanvasElement.prototype.toDataURL = function (...args) {
    try {
      const ctx = this.getContext("2d");
      if (ctx) {
        const imgData = originalGetImageData.call(
          ctx,
          0,
          0,
          this.width,
          this.height
        );
        perturbPixels(imgData, getCanvasId(this));
        ctx.putImageData(imgData, 0, 0);
      }
    } catch {
      // Cross-origin or WebGL canvas — fall through to the original
      // method which will either work or throw its own SecurityError.
    }
    return originalToDataURL.apply(this, args);
  };

  CanvasRenderingContext2D.prototype.getImageData = function (...args) {
    const imgData = originalGetImageData.apply(this, args);
    try {
      perturbPixels(imgData, getCanvasId(this.canvas));
    } catch {
      // Defensive — return unmodified data rather than breaking the site
    }
    return imgData;
  };

  // ─────────────────────────────────────────────────────────────────
  // 2. OS / Hardware Alignment (Navigator + WebGL spoofing)
  // ─────────────────────────────────────────────────────────────────

  // navigator.platform
  Object.defineProperty(Navigator.prototype, "platform", {
    get() {
      return MAC_PROFILE.platform;
    },
    configurable: true,
    enumerable: true,
  });

  // navigator.userAgent
  Object.defineProperty(Navigator.prototype, "userAgent", {
    get() {
      return MAC_PROFILE.userAgent;
    },
    configurable: true,
    enumerable: true,
  });

  // navigator.vendor
  Object.defineProperty(Navigator.prototype, "vendor", {
    get() {
      return MAC_PROFILE.vendor;
    },
    configurable: true,
    enumerable: true,
  });

  // navigator.appVersion (derived from userAgent)
  Object.defineProperty(Navigator.prototype, "appVersion", {
    get() {
      return MAC_PROFILE.userAgent.replace(/^Mozilla\//, "");
    },
    configurable: true,
    enumerable: true,
  });

  // WebGL RENDERER / VENDOR via WEBGL_debug_renderer_info
  const originalGetParameter = WebGLRenderingContext.prototype.getParameter;

  function patchedGetParameter(param) {
    // WEBGL_debug_renderer_info constants
    const UNMASKED_VENDOR = 0x9245;
    const UNMASKED_RENDERER = 0x9246;

    if (param === UNMASKED_VENDOR) return MAC_PROFILE.webglVendor;
    if (param === UNMASKED_RENDERER) return MAC_PROFILE.webglRenderer;

    return originalGetParameter.call(this, param);
  }

  WebGLRenderingContext.prototype.getParameter = patchedGetParameter;

  if (typeof WebGL2RenderingContext !== "undefined") {
    const originalGetParameter2 =
      WebGL2RenderingContext.prototype.getParameter;

    WebGL2RenderingContext.prototype.getParameter = function (param) {
      const UNMASKED_VENDOR = 0x9245;
      const UNMASKED_RENDERER = 0x9246;
      if (param === UNMASKED_VENDOR) return MAC_PROFILE.webglVendor;
      if (param === UNMASKED_RENDERER) return MAC_PROFILE.webglRenderer;
      return originalGetParameter2.call(this, param);
    };
  }

  // ─────────────────────────────────────────────────────────────────
  // 3. UI Widget in Closed Shadow DOM
  // ─────────────────────────────────────────────────────────────────

  function injectStatusWidget() {
    // Host element — intentionally generic tag with no id/class so it
    // doesn't collide with anything on the page.
    const host = document.createElement("privacy-shield-root");

    // Closed shadow — external JS cannot call host.shadowRoot, so
    // neither querySelector nor MutationObserver on the main DOM tree
    // can see the widget internals.
    const shadow = host.attachShadow({ mode: "closed" });

    shadow.innerHTML = `
      <style>
        :host {
          all: initial;
          position: fixed;
          bottom: 12px;
          right: 12px;
          z-index: 2147483647;
          pointer-events: none;
          font-family: -apple-system, BlinkMacSystemFont, sans-serif;
        }
        .shield-widget {
          pointer-events: auto;
          display: flex;
          align-items: center;
          gap: 6px;
          background: rgba(0, 0, 0, 0.72);
          backdrop-filter: blur(8px);
          -webkit-backdrop-filter: blur(8px);
          border-radius: 8px;
          padding: 6px 10px;
          cursor: default;
          user-select: none;
          transition: opacity 0.25s ease;
          opacity: 0.45;
        }
        .shield-widget:hover {
          opacity: 1;
        }
        .shield-icon {
          width: 16px;
          height: 16px;
          flex-shrink: 0;
        }
        .shield-label {
          font-size: 11px;
          color: #4ade80;
          letter-spacing: 0.02em;
          white-space: nowrap;
        }
      </style>

      <div class="shield-widget" title="Privacy Shield active — fingerprint protection enabled">
        <svg class="shield-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M12 2L3 7v5c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V7l-9-5z"
                fill="#4ade80" opacity="0.25"/>
          <path d="M12 2L3 7v5c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V7l-9-5z"
                stroke="#4ade80" stroke-width="1.5" fill="none"/>
          <path d="M9.5 12.5l2 2 4-4" stroke="#4ade80" stroke-width="1.8"
                stroke-linecap="round" stroke-linejoin="round" fill="none"/>
        </svg>
        <span class="shield-label">Shield Active</span>
      </div>
    `;

    // Inject as soon as the body exists
    if (document.body) {
      document.body.appendChild(host);
    } else {
      // document_start may fire before <body> — wait for it
      const observer = new MutationObserver(() => {
        if (document.body) {
          document.body.appendChild(host);
          observer.disconnect();
        }
      });
      observer.observe(document.documentElement, { childList: true });
    }
  }

  // Kick off widget injection
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", injectStatusWidget, {
      once: true,
    });
  } else {
    injectStatusWidget();
  }
})();
