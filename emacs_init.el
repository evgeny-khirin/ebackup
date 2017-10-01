;; Put the following line in your ~/.emacs file
;(load-file (expand-file-name "~/work/bzr_reps/eb/emacs_init.el")) <-- insert your path

;;****************************************************************************
;; Init slime
;;****************************************************************************
(setq inferior-lisp-program "/usr/bin/sbcl"); <-- insert your path
(add-to-list 'load-path (expand-file-name "~/emacs/slime")); <-- insert your path
(add-to-list 'load-path (expand-file-name "~/emacs/slime/contrib")); <-- insert your path
(require 'slime)
(require 'slime-autoloads)
(slime-setup '(slime-fancy slime-scratch slime-editing-commands slime-fuzzy slime-presentations))

(setq slime-net-coding-system 'utf-8-unix)
(setq common-lisp-hyperspec-root (expand-file-name "~/HyperSpec-7-0/HyperSpec")); <-- insert your path

;; init slime for mit-scheme
(setq slime-lisp-implementations
      '((mit-scheme ("mit-scheme") :init mit-scheme-init)))
(add-to-list 'slime-lisp-implementations '(sbcl ("sbcl")))

(defun mit-scheme-init (file encoding)
  (setq slime-protocol-version 'ignore)
  (format "%S\n\n"
    `(begin
      (load-option 'format)
      (load-option 'sos)
      (eval
       '(construct-normal-package-from-description
         (make-package-description '(swank) '(())
           (vector) (vector) (vector) false))
       (->environment '(package)))
      (load ,(expand-file-name
        "~/emacs/slime/contrib/swank-mit-scheme.scm" ; <-- insert your path
        slime-path)
      (->environment '(swank)))
      (eval '(start-swank ,file) (->environment '(swank))))))

(defun mit-scheme ()
  (interactive)
  (slime 'mit-scheme))

(defun find-mit-scheme-package ()
  (save-excursion
    (let ((case-fold-search t))
      (and (re-search-backward "^[;]+ package: \\((.+)\\).*$" nil t)
     (match-string-no-properties 1)))))

(setq slime-find-buffer-package-function 'find-mit-scheme-package)

(require 'slime-scheme)
(slime-scheme-init)

;;****************************************************************************
;; Enable case convertions
;;****************************************************************************
(put 'downcase-region 'disabled nil)
(put 'upcase-region 'disabled nil)

;;****************************************************************************
;; Allow extra space at the end of the line
;;****************************************************************************
(setq-default fill-column 80)

;;****************************************************************************
;; Make lines wrap automatically in text mode.
;;****************************************************************************
(add-hook 'text-mode-hook
          '(lambda () (auto-fill-mode 1)))
(add-hook 'lisp-mode-hook
          '(lambda () (auto-fill-mode 1)))

;;****************************************************************************
;; Setup tab
;;****************************************************************************
;(setq-default indent-tabs-mode nil) ; never use tab character
(setq default-tab-width 2)          ; set tab width

;;****************************************************************************
;; Highlight Current Line
;;****************************************************************************
;(global-hl-line-mode 1)
(global-hl-line-mode 0)

;;****************************************************************************
;; Show line-number in the mode line
;;****************************************************************************
(line-number-mode 1)

;;****************************************************************************
;; Show column-number in the mode line
;;****************************************************************************
(column-number-mode 1)

;****************************************************************************
; Collapse multiple spaces into single one.
;****************************************************************************
(defun my-collapse-whitespace ()
   "Reduce all whitespace surrounding point to a single space."
   ;; @@ This seems to be quite buggy at the moment
   (interactive)
   (kill-region (progn (re-search-backward "[^ \t\r\n]")
                       (forward-char)
                       (point))
                (progn (re-search-forward "[^ \t\r\n]")
                       (backward-char)
                       (point)))
   (insert-char ?\  1))
(global-set-key "\C-cw" 'my-collapse-whitespace)

;;****************************************************************************
;; kill whole line
;;****************************************************************************
(defun my-kill-whole-line ()
   "Kill an entire line, including trailing newline"
   (interactive)
   (beginning-of-line)
   (kill-line 1))
(global-set-key "\C-ck" 'my-kill-whole-line)

;;****************************************************************************
;; goto line function C-c C-g
;;****************************************************************************
(global-set-key [ (control c) (control g) ] 'goto-line)

;;****************************************************************************
;; Comment-uncomment shorcut
;;****************************************************************************
(global-set-key "\C-c;" 'comment-or-uncomment-region)

;****************************************************************************
;; display the current time
;****************************************************************************
;(display-time)

;;****************************************************************************
;; alias y to yes and n to no
;;****************************************************************************
(defalias 'yes-or-no-p 'y-or-n-p)

;;****************************************************************************
;; Remove trailing white spaces and replace all tabs with spaces when saving.
;; WARNING: Can cause problems with makefiles
;;****************************************************************************
(add-hook 'write-file-hooks
  (function (lambda ()
        (delete-trailing-whitespace))))
;        (untabify (point-min) (point-max)))))

;;****************************************************************************
;; Don't make backup files
;;****************************************************************************
(setq make-backup-files nil backup-inhibited t)

;;****************************************************************************
;; Erlang-mode
;;****************************************************************************
(setq load-path (cons "/usr/local/lib/erlang/lib/tools-2.6.2/emacs" load-path)); <-- insert your path
(setq erlang-root-dir "/usr/local/lib/erlang"); <-- insert your path
(setq exec-path (cons "/usr/local/lib/erlang/bin" exec-path)); <-- insert your path
(require 'erlang-start)

(add-hook 'erlang-mode-hook
  (lambda ()
    (setq inferior-erlang-machine-options '("-sname" "emacs@localhost"))
    (imenu-add-to-menubar "imenu")))

;;****************************************************************************
;; Load Distel
;;****************************************************************************
(push (expand-file-name "~/emacs/distel/elisp") load-path); <-- insert your path
(require 'distel)
(distel-setup)

;;****************************************************************************
;; Mousewheel
;;****************************************************************************
(defun sd-mousewheel-scroll-up (event)
  "Scroll window under mouse up by five lines."
  (interactive "e")
  (let ((current-window (selected-window)))
    (unwind-protect
        (progn
          (select-window (posn-window (event-start event)))
          (scroll-up 3))
      (select-window current-window))))

(defun sd-mousewheel-scroll-down (event)
  "Scroll window under mouse down by five lines."
  (interactive "e")
  (let ((current-window (selected-window)))
    (unwind-protect
        (progn
          (select-window (posn-window (event-start event)))
          (scroll-down 3))
      (select-window current-window))))

(global-set-key (kbd "<mouse-5>") 'sd-mousewheel-scroll-up)
(global-set-key (kbd "<mouse-4>") 'sd-mousewheel-scroll-down)

;;****************************************************************************
;; disable colors in emacs
;;****************************************************************************
(global-font-lock-mode nil)

;;****************************************************************************
;; add command line switch for ediif
;; Usage: emacs -diff file1 file2
;;****************************************************************************
(defun command-line-diff (switch)
	(let ((file1 (pop command-line-args-left))
				(file2 (pop command-line-args-left)))
		(ediff file1 file2)))

(add-to-list 'command-switch-alist '("diff" . command-line-diff))

;;****************************************************************************
;; Disable the splash screen
;;****************************************************************************
(setq inhibit-splash-screen 1)