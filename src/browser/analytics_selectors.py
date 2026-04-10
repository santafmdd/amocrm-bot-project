"""Shared selectors/constants for analytics browser flow."""

from __future__ import annotations

TAG_REFERENCE_INPUT_SELECTOR = "input[name='filter[cf][735215]']"

FILTER_OPEN_SELECTORS: tuple[str, ...] = (
    "button:has-text('\u0424\u0438\u043b\u044c\u0442\u0440')",
    "button:has-text('\u0424\u0438\u043b\u044c\u0442\u0440\u044b')",
    "[role='button']:has-text('\u0424\u0438\u043b\u044c\u0442\u0440')",
    "[role='button']:has-text('\u0424\u0438\u043b\u044c\u0442\u0440\u044b')",
    "[class*='filter'] button",
    ".js-stats-filter-button",
    ".sidebar__button_filter",
    "[data-test*='filter']",
    "[data-testid*='filter']",
    "[class*='filter']",
)

APPLY_SELECTORS: tuple[str, ...] = (
    "button:has-text('\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c')",
    "button:has-text('\u041f\u0420\u0418\u041c\u0415\u041d\u0418\u0422\u042c')",
    "[role='button']:has-text('\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c')",
    "button:has-text('Apply')",
    "[role='button']:has-text('Apply')",
)

FILTER_INPUT_SELECTORS: tuple[str, ...] = (
    "input[type='text']",
    "input[placeholder*='\u0412\u0432\u0435\u0434\u0438\u0442\u0435']",
    "input[placeholder*='\u041f\u043e\u0438\u0441\u043a']",
    "[contenteditable='true']",
)

FILTER_KIND_LABELS: dict[str, tuple[str, ...]] = {
    "tag": ("\u0422\u0415\u0413", "\u0422\u0415\u0413\u0418", "TAG"),
    "utm_source": ("UTM_SOURCE", "UTM SOURCE", "UTM", "UTM \u041c\u0415\u0422\u041a\u0410", "UTM-\u041c\u0415\u0422\u041a\u0410"),
}

TAG_PLACEHOLDER_SELECTORS: tuple[str, ...] = (
    "input[placeholder*='\u0422\u0435\u0433\u0438']",
    "textarea[placeholder*='\u0422\u0435\u0433\u0438']",
    "[placeholder*='\u0422\u0435\u0433\u0438']",
    "input[placeholder*='\u0442\u0435\u0433\u0438' i]",
    "textarea[placeholder*='\u0442\u0435\u0433\u0438' i]",
    "[placeholder*='\u0442\u0435\u0433\u0438' i]",
    "div:has([placeholder*='\u0422\u0435\u0433\u0438']) input",
    "div:has([placeholder*='\u0422\u0435\u0433\u0438']) textarea",
)

UTM_HOLDER_SELECTORS: tuple[str, ...] = (
    "div.filter-search__tags-holder[data-input-name*='utm']",
    "div.filter-search__tags-holder[data-title*='UTM' i]",
    "div.filter-search__tags-holder:has-text('UTM')",
    "[data-input-name*='utm_source']",
    "[data-title*='UTM']",
)

UTM_INPUT_SELECTORS: tuple[str, ...] = (
    "input.multisuggest__input.js-multisuggest-input",
    "li.multisuggest__list-item_input input",
    "input[type='text']",
    "input",
    "[contenteditable='true']",
)

FILTER_PANEL_CONTAINER_SELECTORS: tuple[str, ...] = (
    "[role='dialog']",
    "[class*='modal']",
    "[class*='drawer']",
    "[class*='overlay']",
    "[class*='popup']",
    "[class*='filter']",
)

FILTER_PANEL_SCROLLABLE_SELECTORS: tuple[str, ...] = (
    "[class*='scroll']",
    "[class*='content']",
    "[class*='body']",
    "[class*='list']",
    "div",
)

FILTER_PANEL_SELECTOR_DIAGNOSTICS: tuple[str, ...] = (
    "[role='combobox']",
    "[role='option']",
    "[class*='select']",
    "[class*='dropdown']",
    "[class*='suggest']",
    "[class*='filter']",
    "[class*='multiselect']",
    "[class*='control']",
    "[class*='chip']",
    "[class*='tag']",
    "[class*='row']",
    "[contenteditable='true']",
    "input",
    "button",
)

TARGET_PANEL_LABELS: tuple[str, ...] = (
    "tag",
    "\u0442\u0435\u0433",
    "\u0442\u0435\u0433\u0438",
    "\u043c\u0435\u0442\u043a\u0430",
    "\u043c\u0435\u0442\u043a\u0438",
    "utm_source",
    "utm source",
    "utm",
)
