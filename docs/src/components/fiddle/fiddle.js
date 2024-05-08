import { magicElementsAbove, parseSQLTxs, makeError, fiddle_url, runFiddle } from "./utils"

// From my experiments it seems like web-components' constructors can be called in any order
//
// So, the point of this class is twofold:
// 1. To be a singleton per fiddle-embed element
// 2. To contain *only* initialised elements (except txs) to make race conditions explicit
class FiddleRegistry {
    constructor(parent) {
        // Input state
        this.state = {};

        // Txs inside the parent element
        // NOTE: These may not be initialised yet
        // NOTE: We search for these manually rather than register them because the order is important
        this.txs = [];
        for (const el of parent.querySelectorAll("fiddle-txs")) {
            this.txs.push(el);
        }

        // Txs above the parent element
        // NOTE: These not being initialised yet is fine because we can use the dataset to get the original txs
        this.magic_txs = [];
        if (parent.dataset.magicContext) {
            let id = parent.dataset.id;
            let magicContext = parent.dataset.magicContext;
            for (const el of magicElementsAbove(magicContext, id)) {
                for (const txs_el of el.querySelectorAll("fiddle-txs")) {
                    this.magic_txs.push(txs_el);
                }
            }
        }

        // Either a Query editor or a QueryTemplate
        this.query;
        this.isTemplate = false;

        // The list of outputs
        // TODO: Add a default output
        this.outputs = [];
        // An OK heuristic
        function looksLikeOutput(el) {
            let tagName = el.tagName.toLowerCase();
            return tagName.startsWith("fiddle-output");
        }
        let foundOutput = false;
        // We don't use this to find outputs, just to see if we thinkg any
        // will be registered
        for (const el of parent.querySelectorAll('*')) {
            if (looksLikeOutput(el)) {
                foundOutput = true;
                break;
            }
        }
        if (!foundOutput) {
            // Add a default OutputTable
            // NOTE: Delay to avoid recursion in the constructor
            setTimeout(_ => {
                let table_output = document.createElement('fiddle-output-table');
                table_output.style.display = 'none';
                parent.querySelector('[data-id="content"]').appendChild(table_output);
                // Will regester itself
            }, 0);
        }

        // A map of registered callbacks for events
        this.eventCallbacks = {};

        // The error element
        this.errorEl = parent.querySelector('[data-id="fiddle-error"]');
        if (!this.errorEl) {
            throw Error("Must have a child element with data-id fiddle-error");
        }

        // Have the outputs been rendered yet?
        // (To help ensure not to render when we haven't already rendered)
        this.renderedOutputs = false;

        // Reduce re-renders during init by rendering late & registering re-renders late
        setTimeout(() => {
            if (parent.dataset.autoLoad == 'true') {
                this.render();
            } else if (this.query && this.isTemplate) {
                // Always at least render the template
                this.query.render(this.state);
            }

            this.on("setValue", _ => {
                this.render();
            });

            // NOTE: Here we decide what to do when something registers late
            this.on("registerQuery", _ => {
                if (this.renderedOutputs) {
                    this.render();
                }
            });
            this.on("registerTemplate", _ => {
                if (this.renderedOutputs) {
                    this.render();
                } else {
                    // Always at least render self when registered
                    this.query.render(this.state);
                }
            });
            this.on("registerOutput", _ => {
                if (this.renderedOutputs) {
                    this.render();
                }
            });
            this.on("registerInput", _ => {
                if (this.renderedOutputs) {
                    this.render();
                }
            });
            this.on("registerTxs", _ => {
                if (this.renderedOutputs) {
                    this.render();
                }
            });
        // A small hack to try and call this after all/most components have rendered
        }, 100)
    }

    setValue(key, value) {
        this.state[key] = value;
        this._call("setValue");
    }

    // Helper function to display an error
    displayError(title, message, data) {
        // Hide all outputs
        for (const output of this.outputs) {
            output.style.display = 'none';
        }
        // Show error
        this.errorEl.style.display = 'block';
        this.errorEl.innerHTML = makeError(title, message, data);
    }

    render() {
        if (this.query && this.outputs.length != 0) {

            let query;
            if (this.isTemplate) {
                query = this.query.render(this.state);
            } else {
                query = this.query.query;
            }

            let txs = [];
            for (const el of this.magic_txs) {
                // For Txs outside the parent element we use the original txs
                txs.push({
                    // Use dataset to avoid race condition of the element not
                    // being constructed yet
                    txs: parseSQLTxs(el.dataset.txs),
                    'system-time': el.dataset.systemTime || null,
                });
            }
            for (const el of this.txs) {
                if (el.txs) {
                    txs.push({
                        txs: parseSQLTxs(el.txs),
                        'system-time': el.dataset.systemTime || null,
                    });
                } else {
                    // TODO: Decide what to do here, try out:
                    // - Render error
                    // - Do nothing and re-render once registered?
                    // - Do nothing while in init, render error after
                    return;
                }
            }

            this.renderedOutputs = true;

            this.run(query, txs)
                .then(result => {
                    if (result.ok) {
                        this.errorEl.style.display = 'none';
                        for (const output of this.outputs) {
                            output.style.display = 'block';
                            output.render(result.body);
                        }
                    } else {
                        this.displayError(
                            result.body.exception,
                            result.body.message,
                            result.body.data
                        );
                    }
                })
                .catch(error => {
                    var title = "Error";
                    var message = error.message;

                    switch (error.message) {
                        case "Network Error":
                            title = "Network Error";
                            message = "Uh oh! A network error. Please try again.";
                            break;
                        case "JSON Parse Error":
                            title = "JSON Parse Error";
                            message = "Uh oh! Failed to read the result. Please try again or contact us to sort it out.";
                            break;
                    }
                    this.displayError(title, message);
                });
        } else {
            // TODO: Do nothing?
        }
    }

    async run(query, txs) {
        // Run the given txs and query
        try {
            var response = await runFiddle(txs, query);
        } catch (e) {
            throw new Error("Network Error", {error: e});
        }

        try {
            var json = await response.json();
        } catch (e) {
            throw new Error("JSON Parse Error", {response: response, error: e});
        }

        let ret = {
            ok: response.ok,
            body: json
        };

        return ret;
    }

    openInFiddle() {
        let query = "";
        if (this.query) {
            if (this.isTemplate) {
                query = this.query.render(this.state);
            } else {
                query = this.query.query;
            }
        }

        let txs = [];
        for (const el of this.magic_txs) {
            // For Txs outside the parent element we use the original txs
            txs.push({
                // Use dataset to avoid race condition of the element not
                // being constructed yet
                txs: el.dataset.txs,
                'system-time': el.dataset.systemTime || null,
            });
        }
        for (const el of this.txs) {
            txs.push({
                // If not initialised yet, best effort
                txs: el.txs ? el.txs : el.dataset.txs,
                'system-time': el.dataset.systemTime || null,
            });
        }

        var url = new URL(fiddle_url);
        url.searchParams.append('type', 'sql');
        url.searchParams.append('txs', btoa(JSON.stringify(txs)));
        url.searchParams.append('query', btoa(query));

        window.open(url);
    }

    _registerQuery(el) {
        if (this.query) {
            if (this.isTemplate) {
                throw Error("Template already exists in registry");
            } else {
                throw Error("Query already exists in registry");
            }
        } else {
            this.query = el;
        }
    }

    registerQuery(el) {
        this._registerQuery(el);
        this.isTemplate = false;
        this._call("registerQuery");
        return this;
    }

    registerTemplate(el) {
        this._registerQuery(el);
        this.isTemplate = true;
        this._call("registerTemplate");
        return this;
    }

    registerOutput(el) {
        this.outputs.push(el);
        this._call("registerOutput");
        return this;
    }

    registerInput(el) {
        this._call("registerInput");
        return this;
    }

    registerTxs(el) {
        this._call("registerTxs");
        return this;
    }

    _call(event) {
        let callbacks = this.eventCallbacks[event];

        if (callbacks) {
            for (const callback of callbacks) {
                callback(this);
            }
        }
    }

    on(event, callback) {
        if (this.eventCallbacks[event]) {
            this.eventCallbacks[event].push(callback);
        } else {
            this.eventCallbacks[event] = [callback];
        }
    }
}

function makeRegistry() {
    let fiddleRegistry = {};
    let registry = function(el) {
        let parent = el.closest("fiddle-embed");
        if (!parent) {
            throw Error("Must be a child of a fiddle-embed");
        }

        let id = parent.dataset.id;
        if (!id) {
            throw Error("Parent fiddle-embed must have unique data-id set");
        }

        if (!(id in fiddleRegistry)) {
            fiddleRegistry[id] = new FiddleRegistry(parent);
        }
        return fiddleRegistry[id];
    }
    return registry
}

var registry = makeRegistry();
function clearRegistry() {
    registry = makeRegistry();
}

class FiddleComponent extends HTMLElement {
    connectedCallback() {
        // Get the registry
        this._registry = registry(this);
    }
}

class FiddleInput extends FiddleComponent {
    connectedCallback() {
        super.connectedCallback();
        this._registry.registerInput(this);
    }
}

class FiddleOutput extends FiddleComponent {
    connectedCallback() {
        super.connectedCallback();
        this._registry.registerOutput(this);
    }

}

export { registry, clearRegistry, FiddleComponent, FiddleInput, FiddleOutput }
