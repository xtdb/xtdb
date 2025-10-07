import yaml from 'js-yaml';

function dedent(text) {
  const lines = text.split('\n');
  const indents = lines
    .filter(line => line.trim().length > 0)
    .map(line => line.match(/^(\s*)/)[0].length);

  if (indents.length === 0) return text.trim();

  const minIndent = Math.min(...indents);
  const dedented = lines.map(line => line.slice(minIndent)).join('\n');

  return dedented.trim();
}

export function extractSnippets(yamlContent) {
  const parsed = yaml.load(yamlContent);
  const snippets = {};

  for (const [key, value] of Object.entries(parsed)) {
    snippets[key] = dedent(value);
  }

  return snippets;
}
