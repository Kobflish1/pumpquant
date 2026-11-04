// Shared page shell for the overview and the research terminal.
//
//   initShell({ reveal, onThemeChange })
//     - sidebar: full or icons only, remembered between visits
//     - navigation: highlights the section being read and moves a glass thumb to it
//     - theme: light or dark, remembered between visits
//     - toolbar: deeper glass separation while content scrolls beneath it
//     - motion: sections rise in on scroll; cards and glass catch the pointer's
//       light and glow from the point pressed
// Motion is skipped for Reduce Motion or when the URL carries ?still (used for
// reproducible screenshots). Navigation, theme and sidebar always work.

export const reduceMotion = matchMedia("(prefers-reduced-motion: reduce)").matches || new URLSearchParams(location.search).has("still");
export const $ = (selector, root = document) => root.querySelector(selector);
export const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const store = {
  get(key) { try { return localStorage.getItem(key); } catch { return null; } },
  set(key, value) { try { localStorage.setItem(key, value); } catch { /* storage unavailable */ } },
};

/* ---------- Sidebar ---------- */

const thumb = Object.assign(document.createElement("span"), { className: "nav-thumb" });

function placeThumb() {
  const active = $(".nav a.active");
  thumb.style.opacity = active ? "1" : "0";
  if (active) thumb.style.transform = `translateY(${active.offsetTop}px)`;
}

function sidebar() {
  const shell = $(".shell"), button = $("#sidebar-toggle");
  if (!shell || !button) return;
  const links = $$(".nav a, .side-link");
  const apply = (collapsed) => {
    shell.classList.toggle("collapsed", collapsed);
    button.setAttribute("aria-expanded", String(!collapsed));
    button.setAttribute("aria-label", collapsed ? "Expand sidebar" : "Collapse sidebar");
    button.title = collapsed ? "Expand sidebar" : "Collapse sidebar";
    links.forEach((link) => { if (collapsed) link.title = link.textContent.trim(); else link.removeAttribute("title"); });
    requestAnimationFrame(placeThumb);
  };
  let collapsed = store.get("pumpquant.sidebar") === "collapsed";
  apply(collapsed);
  button.addEventListener("click", () => { collapsed = !collapsed; apply(collapsed); store.set("pumpquant.sidebar", collapsed ? "collapsed" : "expanded"); });
  shell.addEventListener("transitionend", (event) => { if (event.target === shell) placeThumb(); });
}

/* ---------- Sections: highlight what is being read ---------- */

function sections() {
  const nav = $(".nav");
  if (!nav) return;
  nav.prepend(thumb);
  const links = new Map($$(".nav a[href^='#']").map((a) => [a.getAttribute("href").slice(1), a]));
  const title = $("#topbar-title");
  const spy = new IntersectionObserver((entries) => {
    const visible = entries.filter((e) => e.isIntersecting).sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
    if (!visible || !links.has(visible.target.id)) return;
    links.forEach((a, id) => a.classList.toggle("active", id === visible.target.id));
    const name = links.get(visible.target.id).textContent.trim();
    if (title && title.textContent !== name) {
      title.style.opacity = "0";
      setTimeout(() => { title.textContent = name; title.style.opacity = "1"; }, reduceMotion ? 0 : 160);
    }
    placeThumb();
  }, { rootMargin: "-35% 0px -55% 0px" });
  links.forEach((_, id) => { const section = document.getElementById(id); if (section) spy.observe(section); });
  placeThumb();
  addEventListener("resize", placeThumb);

  const topbar = $(".topbar");
  const over = () => topbar?.classList.toggle("over", scrollY > 8);
  addEventListener("scroll", over, { passive: true });
  over();
}

/* ---------- Theme ---------- */

function theme(onChange) {
  const root = document.documentElement;
  const saved = store.get("pumpquant.theme");
  if (saved) root.dataset.theme = saved;
  const button = $("#theme-toggle");
  button?.addEventListener("click", () => {
    const flip = () => {
      root.dataset.theme = root.dataset.theme === "light" ? "dark" : "light";
      store.set("pumpquant.theme", root.dataset.theme);
      onChange?.();
    };
    if (reduceMotion || !document.startViewTransition) { flip(); return; }
    // Apple-style appearance change: one unified crossfade. Element transitions
    // are paused during the swap so every surface re-tints in the same instant.
    root.classList.add("theme-switching");
    const transition = document.startViewTransition(flip);
    transition.finished.finally(() => root.classList.remove("theme-switching"));
  });
}

/* ---------- Motion ---------- */

const LIT = ".card, .glass, .panel, .headline-card, .cost-ribbon";

function light() {
  // The surface currently catching the light. Leaving it must clear both the
  // marker and the stored position: glass draws its highlight from --mx/--my.
  let current = null;
  const release = () => {
    if (!current) return;
    current.classList.remove("lit");
    current.style.removeProperty("--mx");
    current.style.removeProperty("--my");
    current = null;
  };
  document.documentElement.addEventListener("pointerleave", release);
  document.addEventListener("pointermove", (event) => {
    const surface = event.target.closest?.(LIT);
    if (surface !== current) { release(); current = surface; }
    if (!surface) return;
    const rect = surface.getBoundingClientRect();
    surface.style.setProperty("--mx", `${event.clientX - rect.left}px`);
    surface.style.setProperty("--my", `${event.clientY - rect.top}px`);
    surface.classList.add("lit");
  });
  document.addEventListener("pointerdown", (event) => {
    const surface = event.target.closest?.(LIT);
    if (!surface || event.target.closest("input, select, textarea, .chart, .token-chart")) return;
    const rect = surface.getBoundingClientRect();
    ["bloom", "core"].forEach((kind) => {
      const spot = Object.assign(document.createElement("span"), { className: `glow-spot ${kind}` });
      spot.style.left = `${event.clientX - rect.left}px`;
      spot.style.top = `${event.clientY - rect.top}px`;
      surface.append(spot);
      spot.addEventListener("animationend", () => spot.remove());
    });
    if (surface.matches(".capsule")) { surface.classList.remove("flex"); void surface.offsetWidth; surface.classList.add("flex"); }
  });
  document.addEventListener("animationend", (event) => { if (event.animationName === "gel") event.target.classList.remove("flex"); });
}

/** Make elements rise into place as they scroll into view. */
export function revealOnScroll(selector, onReveal) {
  if (reduceMotion) return;
  const reveal = new IntersectionObserver((entries) => {
    let k = 0;
    entries.filter((e) => e.isIntersecting).forEach((e) => {
      e.target.style.setProperty("--delay", `${Math.min(k++, 6) * 60}ms`);
      e.target.classList.add("in");
      onReveal?.(e.target);
      reveal.unobserve(e.target);
    });
  }, { rootMargin: "0px 0px -10% 0px" });
  $$(selector).forEach((el) => { if (!el.classList.contains("reveal")) { el.classList.add("reveal"); reveal.observe(el); } });
}

/** Resolve a heading word by word, like keynote type. */
export function resolveWords(element) {
  if (reduceMotion || !element) return;
  element.classList.add("words");
  element.innerHTML = element.textContent.split(/(\s+)/).map((w, i) => `<span style="--i:${i}">${w}</span>`).join("");
  requestAnimationFrame(() => requestAnimationFrame(() => element.classList.add("in")));
}

/**
 * Chart labels people can read. Real timestamps show the local time of day. The demo's
 * clock has no calendar date, so its charts start at the token's launch and are labelled
 * with the time since then ("0 s", "45 s", "2 min 30 s") rather than as times in 1970.
 */
export function chartTime(time) {
  const seconds = typeof time === "number" ? time : Date.UTC(time.year, time.month - 1, time.day) / 1000;
  if (seconds < 1e9) return sinceLaunch(seconds);
  return new Date(seconds * 1000).toLocaleTimeString("en", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

function sinceLaunch(total) {
  const minutes = Math.floor(total / 60), seconds = Math.round(total % 60);
  if (minutes === 0) return `${seconds} s`;
  return seconds ? `${minutes} min ${seconds} s` : `${minutes} min`;
}

/** Whole percentages with a true minus sign, for percentage price scales. */
export function chartPercent(value) {
  const rounded = Math.round(value);
  return `${rounded < 0 ? "−" : ""}${Math.abs(rounded)}%`;
}

export function initShell({ onThemeChange } = {}) {
  theme(onThemeChange);
  sidebar();
  sections();
  if (reduceMotion) document.documentElement.classList.add("motion-off");
  else light();
}
