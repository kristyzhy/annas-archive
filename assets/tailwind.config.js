const { addDynamicIconSelectors } = require('@iconify/tailwind');

module.exports = {
  content: [
    '/app/assets/js/**/*.js',
    '/app/assets/css/**/*.css',
    '/app/allthethings/**/*.html'
  ],
  plugins: [
    addDynamicIconSelectors(),
  ],
}
