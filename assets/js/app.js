import emailMisspelled, { microsoft, all } from "email-misspelled";
import AriaTablist from 'aria-tablist';
import Plotly from 'plotly.js-dist-min'

window.Plotly = Plotly;


const microsoftWithMsn = microsoft.concat(
    microsoft.filter(e => e.includes('hotmail')).map(e => e.replace('hotmail', 'msn'))
);
window.emailMisspelled = {
    emailMisspelled, microsoft: microsoftWithMsn, all
};

document.addEventListener("DOMContentLoaded", () => {
    for (const el of document.querySelectorAll('[role="tablist"]')) {
        AriaTablist(el, {
            onOpen: (panel, tab) => {
                panel.dispatchEvent(new Event("panelOpen"));
            },
        });
    }
});

// https://stackoverflow.com/a/69190644
window.executeScriptElements = (containerElement) => {
  const scriptElements = containerElement.querySelectorAll("script");

  Array.from(scriptElements).forEach((scriptElement) => {
    const clonedElement = document.createElement("script");

    Array.from(scriptElement.attributes).forEach((attribute) => {
      clonedElement.setAttribute(attribute.name, attribute.value);
    });
    
    clonedElement.text = scriptElement.text;

    scriptElement.parentNode.replaceChild(clonedElement, scriptElement);
  });
}
