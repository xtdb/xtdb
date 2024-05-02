class LiveQueryInput extends HTMLElement {
    constructor() {
        super();
        this.parent = this.closest("live-query");
        console.assert(this.parent, "Must be a child of a live-query");
    }
}

export { LiveQueryInput };
