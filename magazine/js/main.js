// Mobile nav toggle
document.addEventListener('DOMContentLoaded', function() {
  const toggle = document.querySelector('.nav-toggle');
  const nav = document.querySelector('.main-nav');

  if (toggle) {
    toggle.addEventListener('click', function() {
      nav.classList.toggle('open');
    });
  }

  // Mobile dropdown toggle
  document.querySelectorAll('.dropdown > a').forEach(function(link) {
    link.addEventListener('click', function(e) {
      if (window.innerWidth <= 768) {
        e.preventDefault();
        this.parentElement.classList.toggle('open');
      }
    });
  });

  // Close nav on link click (mobile)
  document.querySelectorAll('.main-nav a:not(.dropdown > a)').forEach(function(link) {
    link.addEventListener('click', function() {
      if (window.innerWidth <= 768) {
        nav.classList.remove('open');
      }
    });
  });
});
