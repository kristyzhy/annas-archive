import emailMisspelled, { microsoft, all } from "email-misspelled";
import AriaTablist from 'aria-tablist';
import Plotly from 'plotly.js-dist-min'

window.Plotly = Plotly;

window.emailMisspelled = {
    emailMisspelled, microsoft, all
};

document.addEventListener("readystatechange", (event) => {
  if (event.target.readyState === "interactive") {
    for (const el of document.querySelectorAll('[role="tablist"]')) {
        AriaTablist(el, {
            onOpen: (panel, tab) => {
                panel.dispatchEvent(new Event("panelOpen"));
            },
        });
    }
  }
});
