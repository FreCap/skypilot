import React, {
  useCallback,
  useEffect,
  useRef,
  createContext,
  useContext,
  useState,
} from 'react';
import { useRouter } from 'next/router';
import Shepherd from 'shepherd.js';
import { createTourSteps } from '@/hooks/tourSteps';
import { getNonce } from '../utils/csp';
import { useFirstVisit } from '@/hooks/useFirstVisit';

const TourContext = createContext(null);

export function useTour() {
  const context = useContext(TourContext);
  if (!context) {
    throw new Error('useTour must be used within a TourProvider');
  }
  return context;
}

// Global function for copying code blocks in tour
if (typeof window !== 'undefined') {
  window['copyDashboardCodeBlock'] = function (button) {
    const codeContainer = button.closest('.bg-gray-50').querySelector('pre');
    const codeBlock = codeContainer.querySelector('code.block');
    const text = codeBlock.textContent;
    navigator.clipboard.writeText(text).then(() => {
      const originalSvg = button.innerHTML;
      button.innerHTML =
        '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-check h-5 w-5 text-green-600"><path d="m9 12 2 2 4-4"/></svg>';
      setTimeout(() => {
        button.innerHTML = originalSvg;
      }, 2000);
    });
  };
}

export function TourProvider({ children }) {
  const tourRef = useRef(null);
  const router = useRouter();
  const { isFirstVisit, markTourCompleted } = useFirstVisit();
  const [tourAutoStarted, setTourAutoStarted] = useState(false);
  const [isTourActive, setIsTourActive] = useState(false);
  const [tourJustStarted, setTourJustStarted] = useState(false);
  const tourNavigatingRef = useRef(false);

  const startTour = useCallback(() => {
    if (tourRef.current) {
      setIsTourActive(true);
      setTourJustStarted(true);
      // Remove delay for immediate tour start since first step doesn't require setup
      tourRef.current.start();

      // Clear the "just started" flag after a delay to allow for initial setup
      setTimeout(() => {
        setTourJustStarted(false);
      }, 1000);
    }
  }, []);

  useEffect(() => {
    // Initialize the tour only once
    if (!tourRef.current) {
      tourRef.current = new Shepherd.Tour({
        useModalOverlay: false,
        defaultStepOptions: {
          cancelIcon: {
            enabled: true,
          },
          scrollTo: { behavior: 'smooth', block: 'center' },
          arrow: false,
          highlightClass: 'shepherd-highlight',
          when: {
            show() {
              const currentStep = Shepherd.activeTour?.getCurrentStep();
              const currentStepElement = currentStep?.getElement();
              const footer =
                currentStepElement?.querySelector('.shepherd-footer');
              const progress = document.createElement('span');
              progress.className = 'shepherd-progress';
              progress.innerText = `${Shepherd.activeTour?.steps.indexOf(currentStep) + 1} of ${Shepherd.activeTour?.steps.length}`;
              footer?.insertBefore(progress, footer.firstChild);

              // Set CSS custom property for dialog height to help mobile menu positioning
              if (currentStepElement) {
                const dialogHeight = currentStepElement.offsetHeight;
                document.documentElement.style.setProperty(
                  '--shepherd-dialog-height',
                  `${dialogHeight + 20}px`
                );

                // Programmatically adjust mobile menu height for better reliability
                if (window.innerWidth < 768) {
                  // Try multiple ways to find the mobile menu
                  let mobileMenu = null;
                  const selectors = [
                    '.fixed.top-14.left-0.w-64',
                    'div.fixed.w-64.bg-white.border-r',
                    '.fixed.w-64.transform',
                    '[class*="fixed"][class*="w-64"][class*="bg-white"]',
                    'div[class*="fixed"][class*="top-14"][class*="left-0"][class*="w-64"]',
                  ];

                  for (const selector of selectors) {
                    mobileMenu = document.querySelector(selector);
                    if (mobileMenu) break;
                  }

                  // If still not found, try finding by position and size
                  if (!mobileMenu) {
                    const allDivs = document.querySelectorAll('div.fixed');
                    for (const div of allDivs) {
                      const rect = div.getBoundingClientRect();
                      if (
                        rect.width === 256 &&
                        rect.left === 0 &&
                        rect.top >= 50
                      ) {
                        // w-64 = 256px
                        mobileMenu = div;
                        break;
                      }
                    }
                  }

                  if (mobileMenu && mobileMenu instanceof HTMLElement) {
                    // Calculate available height from top bar to dialog top
                    const dialogRect =
                      currentStepElement.getBoundingClientRect();
                    const topBarHeight = 56;
                    const availableHeight = dialogRect.top - topBarHeight;

                    // Use direct pixel height instead of calc() to avoid calc issues
                    mobileMenu.style.setProperty(
                      'height',
                      `${availableHeight}px`,
                      'important'
                    );
                    mobileMenu.style.setProperty(
                      'max-height',
                      `${availableHeight}px`,
                      'important'
                    );
                  }
                }
              }

              // Add custom highlight styling to the target element
              const targetElement = currentStep?.getTarget();
              if (targetElement && targetElement instanceof HTMLElement) {
                targetElement.style.outline = '3px solid #3b82f6';
                targetElement.style.outlineOffset = '2px';
                targetElement.style.borderRadius = '8px';
                targetElement.style.position = 'relative';
                targetElement.style.zIndex = '9999';
                targetElement.setAttribute('data-shepherd-highlighted', 'true');
              }
            },
            hide() {
              // Remove custom highlight styling when step is hidden
              const targetElement = document.querySelector(
                '[data-shepherd-highlighted="true"]'
              );
              if (targetElement && targetElement instanceof HTMLElement) {
                targetElement.style.outline = '';
                targetElement.style.outlineOffset = '';
                targetElement.style.borderRadius = '';
                targetElement.style.boxShadow = '';
                targetElement.style.position = '';
                targetElement.style.zIndex = '';
                targetElement.removeAttribute('data-shepherd-highlighted');
              }

              // Clean up CSS custom property for dialog height
              document.documentElement.style.removeProperty(
                '--shepherd-dialog-height'
              );

              // Restore mobile menu height
              const mobileMenu =
                document.querySelector('.fixed.top-14.left-0.w-64') ||
                document.querySelector('div.fixed.w-64.bg-white.border-r') ||
                document.querySelector('.fixed.w-64.transform') ||
                document.querySelector(
                  '[class*="fixed"][class*="w-64"][class*="bg-white"]'
                );
              if (mobileMenu && mobileMenu instanceof HTMLElement) {
                mobileMenu.style.removeProperty('height');
                mobileMenu.style.removeProperty('max-height');
              }
            },
          },
        },
      });

      // Add global CSS styling for tour
      const globalStyle = document.createElement('style');
      globalStyle.id = 'shepherd-global-custom-style';
      // Propagate CSP nonce so the dynamic <style> is not blocked.
      const nonce = getNonce();
      if (nonce) {
        globalStyle.nonce = nonce;
      }
      globalStyle.textContent = `
          .shepherd-element {
            /* Uniform 1px border using inner box-shadow so corners stay consistent */
            border: none !important;
            border-radius: 10px !important;
            z-index: 30000 !important;
            box-shadow: 0 0 0 1px #d1d5db inset, 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05) !important;
            overflow: visible !important;
            background-clip: padding-box !important;
          }

          .shepherd-title {
              font-weight: bold;
              color: #111827;
              margin: 0;
          }

          .shepherd-element .shepherd-header {
              padding: 1rem 1rem 0.5rem 1rem;
          }

          .shepherd-element .shepherd-text {
              padding: 0.5rem 1rem 1rem 1rem;
          }

          /* Fix mobile menu gap when tour dialog is present */
          @media (max-width: 767px) {
            /* Very specific selector to override Tailwind's h-[calc(100vh-56px)] class */
            div.fixed.top-14.left-0.w-64.bg-white.border-r.shadow-lg.z-50.transform,
            .fixed.top-14.left-0.w-64.bg-white.shadow-lg.z-50,
            div[class*="fixed"][class*="top-14"][class*="left-0"][class*="w-64"][class*="bg-white"][class*="shadow-lg"][class*="z-50"] {
              height: calc(100vh - 56px - var(--shepherd-dialog-height, 200px)) !important;
              max-height: calc(100vh - 56px - var(--shepherd-dialog-height, 200px)) !important;
            }

            /* Target the mobile menu by its exact class combination from the HTML */
            .fixed.w-64.bg-white.border-r.border-gray-200.shadow-lg.z-50.transform,
            .fixed[class*="w-64"][class*="bg-white"][class*="border-r"][class*="shadow-lg"][class*="z-50"][class*="transform"] {
              height: calc(100vh - 56px - var(--shepherd-dialog-height, 200px)) !important;
            }

            /* Even more specific - target by multiple class combinations */
            .fixed.top-14.left-0[class*="w-64"],
            div.fixed[class*="top-14"][class*="left-0"][class*="w-64"] {
              height: calc(100vh - 56px - var(--shepherd-dialog-height, 200px)) !important;
            }

            /* Super aggressive approach - use high specificity to override Tailwind */
            body div.fixed.w-64:not(.shepherd-element),
            html body div.fixed.w-64:not(.shepherd-element) {
              height: calc(100vh - 56px - var(--shepherd-dialog-height, 200px)) !important;
            }

            /* Fallback selectors for other mobile menu patterns */
            nav[data-state="open"],
            .mobile-menu.open,
            [data-mobile-menu="true"] {
              height: calc(100vh - var(--shepherd-dialog-height, 200px)) !important;
              max-height: calc(100vh - var(--shepherd-dialog-height, 200px)) !important;
            }

            /* Ensure mobile menu content flows properly */
            .fixed.w-64 nav,
            .fixed[class*="w-64"] nav {
              height: 100% !important;
              overflow-y: auto !important;
            }
          }

        `;
      if (!document.getElementById('shepherd-global-custom-style')) {
        document.head.appendChild(globalStyle);
      }

      // Add tour event listeners
      tourRef.current.on('complete', () => {
        // Remove any remaining highlights
        const targetElement = document.querySelector(
          '[data-shepherd-highlighted="true"]'
        );
        if (targetElement && targetElement instanceof HTMLElement) {
          targetElement.style.outline = '';
          targetElement.style.outlineOffset = '';
          targetElement.style.borderRadius = '';
          targetElement.style.boxShadow = '';
          targetElement.style.position = '';
          targetElement.style.zIndex = '';
          targetElement.removeAttribute('data-shepherd-highlighted');
        }
        // Remove column overlay and related elements
        const overlay = document.getElementById('shepherd-column-overlay');
        if (overlay) {
          overlay.remove();
        }
        const anchorPoint = document.getElementById('shepherd-column-anchor');
        if (anchorPoint) {
          anchorPoint.remove();
        }
        // Remove user column overlay and related elements
        const userOverlay = document.getElementById(
          'shepherd-user-column-overlay'
        );
        if (userOverlay) {
          userOverlay.remove();
        }
        const userAnchorPoint = document.getElementById(
          'shepherd-user-column-anchor'
        );
        if (userAnchorPoint) {
          userAnchorPoint.remove();
        }
        const globalStyle = document.getElementById(
          'shepherd-global-custom-style'
        );
        if (globalStyle) {
          globalStyle.remove();
        }
        // Clean up CSS custom property for dialog height
        document.documentElement.style.removeProperty(
          '--shepherd-dialog-height'
        );

        // Restore mobile menu height
        const mobileMenu =
          document.querySelector('.fixed.top-14.left-0.w-64') ||
          document.querySelector('div.fixed.w-64.bg-white.border-r') ||
          document.querySelector('.fixed.w-64.transform') ||
          document.querySelector(
            '[class*="fixed"][class*="w-64"][class*="bg-white"]'
          );
        if (mobileMenu && mobileMenu instanceof HTMLElement) {
          mobileMenu.style.removeProperty('height');
          mobileMenu.style.removeProperty('max-height');
        }

        setIsTourActive(false);
        setTourJustStarted(false);
        markTourCompleted();
      });

      tourRef.current.on('cancel', () => {
        // Remove any remaining highlights when tour is cancelled
        const targetElement = document.querySelector(
          '[data-shepherd-highlighted="true"]'
        );
        if (targetElement && targetElement instanceof HTMLElement) {
          targetElement.style.outline = '';
          targetElement.style.outlineOffset = '';
          targetElement.style.borderRadius = '';
          targetElement.style.boxShadow = '';
          targetElement.style.position = '';
          targetElement.style.zIndex = '';
          targetElement.removeAttribute('data-shepherd-highlighted');
        }
        // Remove column overlay and related elements
        const overlay = document.getElementById('shepherd-column-overlay');
        if (overlay) {
          overlay.remove();
        }
        const anchorPoint = document.getElementById('shepherd-column-anchor');
        if (anchorPoint) {
          anchorPoint.remove();
        }
        // Remove user column overlay and related elements
        const userOverlay = document.getElementById(
          'shepherd-user-column-overlay'
        );
        if (userOverlay) {
          userOverlay.remove();
        }
        const userAnchorPoint = document.getElementById(
          'shepherd-user-column-anchor'
        );
        if (userAnchorPoint) {
          userAnchorPoint.remove();
        }
        const globalStyle = document.getElementById(
          'shepherd-global-custom-style'
        );
        if (globalStyle) {
          globalStyle.remove();
        }
        // Clean up CSS custom property for dialog height
        document.documentElement.style.removeProperty(
          '--shepherd-dialog-height'
        );

        // Restore mobile menu height
        const mobileMenu =
          document.querySelector('.fixed.top-14.left-0.w-64') ||
          document.querySelector('div.fixed.w-64.bg-white.border-r') ||
          document.querySelector('.fixed.w-64.transform') ||
          document.querySelector(
            '[class*="fixed"][class*="w-64"][class*="bg-white"]'
          );
        if (mobileMenu && mobileMenu instanceof HTMLElement) {
          mobileMenu.style.removeProperty('height');
          mobileMenu.style.removeProperty('max-height');
        }

        setIsTourActive(false);
        setTourJustStarted(false);
        markTourCompleted();
      });

      // Define tour steps
      const steps = createTourSteps(router, tourNavigatingRef);

      // Add steps to the tour
      steps.forEach((step) => {
        tourRef.current.addStep(step);
      });
    }

    if (isFirstVisit && !tourAutoStarted) {
      startTour();
      setTourAutoStarted(true);
    }

    return () => {
      // Cleanup tour on unmount
      if (tourRef.current) {
        tourRef.current.complete();
      }
    };
    // markTourCompleted/router/tourAutoStarted used in step callbacks, not as effect deps
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isFirstVisit, startTour]);

  // Block navigation during tour
  useEffect(() => {
    if (!isTourActive || tourJustStarted) return;

    // Block router navigation unless it's tour-initiated
    const handleRouteChangeStart = (url) => {
      if (!tourNavigatingRef.current) {
        // Show confirmation dialog
        const shouldLeave = window.confirm(
          'The tour is currently in progress. Do you want to abort the tour and navigate away?\n\nYou can resume the tour by clicking the question mark on the bottom right.'
        );
        if (!shouldLeave) {
          router.events.emit('routeChangeError');
          throw 'Route change aborted by user during tour.';
        } else {
          // User wants to leave, cancel the tour
          if (tourRef.current) {
            tourRef.current.cancel();
          }
        }
      }
    };

    // Warn on page refresh/close
    const handleBeforeUnload = (e) => {
      e.preventDefault();
      e.returnValue =
        'The tour is currently in progress. Are you sure you want to leave?';
      return e.returnValue;
    };

    router.events.on('routeChangeStart', handleRouteChangeStart);
    window.addEventListener('beforeunload', handleBeforeUnload);

    return () => {
      router.events.off('routeChangeStart', handleRouteChangeStart);
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [isTourActive, tourJustStarted, router]);

  const completeTour = () => {
    if (tourRef.current) {
      tourRef.current.complete();
    }
    setTourJustStarted(false);
  };

  const value = {
    startTour,
    completeTour,
    tour: tourRef.current,
  };

  return <TourContext.Provider value={value}>{children}</TourContext.Provider>;
}
