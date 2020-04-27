const queryParams = window.location.search;
const path = window.location.pathname;
window.location.replace('/ui' + path + queryParams);
