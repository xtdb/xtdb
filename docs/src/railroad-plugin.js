import { visit } from 'unist-util-visit';
import rr from '../lib/railroad/railroad.js';

export function railroadPlugin() {
  return (tree) => {
    visit(tree, 'code', (node) => {
      if (node.lang === 'railroad') {
        try {
          // Execute the railroad JavaScript code to generate the diagram
          const diagramFunction = eval(`(rr) => {${node.value}}`);
          const diagram = diagramFunction(rr);

          // Generate the SVG string
          const svgString = diagram.toString();

          // Replace the code block with the generated SVG
          node.type = 'html';
          node.value = svgString;
          delete node.lang;
        } catch (error) {
          console.warn(`Failed to render railroad diagram: ${error.message}`);
          // Keep the original code block if rendering fails
        }
      }
    });
  };
}