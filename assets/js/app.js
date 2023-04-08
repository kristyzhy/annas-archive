import emailMisspelled, { microsoft, all } from "email-misspelled";
import AriaTablist from 'aria-tablist';

window.emailMisspelled = {
    emailMisspelled, microsoft, all
};

document.addEventListener("readystatechange", (event) => {
  if (event.target.readyState === "interactive") {
    for (const el of document.querySelectorAll('[role="tablist"]')) {
        AriaTablist(el);
    }
  }
});
