export function getTheme(): string | null {
    return document.querySelector('[data-theme]').getAttribute('data-theme');
}

export function onThemeChange(f: (newtheme: string) => void) {
    const observer = new MutationObserver(function (mutationList, _observer) {
        for (const mutation of mutationList) {
            if (mutation.type === 'attributes' && mutation.attributeName === 'data-theme') {
                let newTheme = mutation.target.getAttribute('data-theme');
                f(newTheme);
            }
        }
    });
    observer.observe(document.querySelector('[data-theme]'), {
        attributes: true,
    });
}
