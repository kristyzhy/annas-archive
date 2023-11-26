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
  theme: {
    extend: {
      opacity: {
        '6.7': '.067',
        '64': '.64',
      }
    },
  },
}
