// Empty states for the research terminal.
//
// Several tools need recorded market data or a full research run. When a
// section has nothing to show (every table holds only its placeholder row and
// every headline figure is "—"), it is replaced by a short explanation of what
// the tool does and what it needs. As soon as data arrives, the real view
// returns: the check re-runs whenever terminal.js updates the page.

const EMPTY = {
  research: {
    icon: '<path d="M5 7h14M5 12h14M5 17h9"/>',
    title: "No strategies published yet",
    text: "When a research run finishes, every strategy it tested appears here, ranked by the same after-cost measures. Nothing is hidden or combined.",
  },
  atlas: {
    icon: '<circle cx="12" cy="12" r="8.5"/><path d="M15.5 8.5l-2 5-5 2 2-5z"/>',
    title: "Token explorer needs recorded market history",
    text: "With a recorded Pump.fun archive, this shows every token launch, how often tokens reached 2×, 5× or 10×, and which early signals came before big moves. The demo runs on simulated data, so there is nothing to explore here.",
  },
  execution: {
    icon: '<path d="M4 17l5-5 4 4 7-8"/><path d="M15 8h5v5"/>',
    title: "Capital simulation needs a full research run",
    text: "Shows how one shared wallet would have handled overlapping trades with real timing, and how much each strategy could trade per day after costs. The demo run stops before this step.",
  },
  validation: {
    icon: '<path d="M12 3.5l7 3v5.5c0 4.2-3 7.4-7 8.5-4-1.1-7-4.3-7-8.5V6.5z"/><path d="M8.8 12.2l2.2 2.2 4.4-4.6"/>',
    title: "No checks have run yet",
    text: "A strategy must pass each check in order before it can be trusted. The final test data stays sealed until it does.",
  },
};

function hasContent(section) {
  const rows = [...section.querySelectorAll("tbody tr")].filter((tr) => !tr.classList.contains("empty-row"));
  const figures = [...section.querySelectorAll(".headline-card strong")].filter((el) => !/^[—–-]?$/.test(el.textContent.trim()));
  const items = section.querySelectorAll(".gate-card, .ladder-row, .log-line, .brief-source");
  return rows.length > 0 || figures.length > 0 || items.length > 0;
}

function decorate(section, { icon, title, text }) {
  if (section.querySelector(".section-empty")) return;
  const box = document.createElement("div");
  box.className = "section-empty";
  box.innerHTML = `<span class="glyph"><svg viewBox="0 0 24 24" aria-hidden="true">${icon}</svg></span><div><strong>${title}</strong><p>${text}</p></div>`;
  section.querySelector(".section-head")?.after(box);
}

function update() {
  // The research plan only appears once a plan has been written down.
  const brief = document.querySelector(".research-brief-panel");
  if (brief) brief.hidden = !brief.querySelector(".brief-source");
  Object.entries(EMPTY).forEach(([id, copy]) => {
    const section = document.getElementById(id);
    if (!section) return;
    decorate(section, copy);
    section.classList.toggle("is-empty", !hasContent(section));
  });
}

let pending = 0;
const schedule = () => { cancelAnimationFrame(pending); pending = requestAnimationFrame(update); };
new MutationObserver(schedule).observe(document.querySelector("main"), { childList: true, subtree: true, characterData: true });
schedule();
