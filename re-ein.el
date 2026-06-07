;;; re-ein.el --- Send Python code to Jupyter API server -*- lexical-binding: t; -*-

(require 'url)
(require 'json)
(require 'ansi-color)

(defgroup re-ein nil
  "Jupyter API client for Emacs."
  :group 'tools)

(defcustom re-ein-server-url "http://localhost:8000"
  "URL of the Jupyter API server."
  :type 'string
  :group 're-ein)

(defcustom re-ein-timeout 86400.0
  "Default timeout for code execution in seconds (24 hours)."
  :type 'number
  :group 're-ein)

(defcustom re-ein-image-max-width nil
  "Maximum width for displayed images in pixels.
nil means render at native size."
  :type '(choice (integer :tag "Maximum width")
                 (const :tag "No limit" nil))
  :group 're-ein)

(defcustom re-ein-save-images nil
  "If non-nil, automatically save images to files."
  :type 'boolean
  :group 're-ein)

(defcustom re-ein-image-directory "~/jupyter-images/"
  "Directory to save images when `re-ein-save-images' is non-nil."
  :type 'directory
  :group 're-ein)

(defvar re-ein-output-buffer-name "*Jupyter Output*"
  "Name of the buffer to display Jupyter output.")

(defvar re-ein-history nil
  "History of executed code snippets.")

(defvar re-ein-stream-process nil
  "Currently running streaming execution process, if any.")

(defface re-ein-running-face
  '((((background dark))  :background "#3a3a00" :extend t)
    (((background light)) :background "#fff5b0" :extend t))
  "Face used to highlight the source region currently being executed."
  :group 're-ein)

(defvar re-ein--running-overlay nil
  "Overlay over the source region currently executing, if any.")

(defun re-ein--write-header (text)
  "Replace the first line of the output buffer with TEXT (a propertized string)."
  (let ((buffer (get-buffer re-ein-output-buffer-name)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (let ((inhibit-read-only t))
          (save-excursion
            (goto-char (point-min))
            (delete-region (point-min) (line-end-position))
            (insert text)))))))

(defun re-ein--start-indicator (out-buffer)
  "Insert a static [...] header into OUT-BUFFER. No timer."
  (with-current-buffer out-buffer
    (let ((inhibit-read-only t))
      (goto-char (point-min))
      (insert (propertize "[·]" 'face 'warning) "\n\n"))))

(defun re-ein--stop-indicator ()
  "Flip the [...] header to [done]. No-op if no [..] header is present, so
calling this defensively before a new run is safe."
  (let ((buffer (get-buffer re-ein-output-buffer-name)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (when (and (> (point-max) 1)
                   (save-excursion
                     (goto-char (point-min))
                     (eq (char-after) ?\[)))
          (re-ein--write-header
           (propertize "[x]" 'face 'success)))))))

(defun re-ein--mark-running (out-buffer src-buffer start end)
  "Indicate that SRC-BUFFER's region START..END is being executed, and
animate a spinner header in OUT-BUFFER."
  (re-ein--clear-running)
  (when (and src-buffer (buffer-live-p src-buffer) start end)
    (let ((ov (make-overlay start end src-buffer)))
      (overlay-put ov 'face 're-ein-running-face)
      (overlay-put ov 'priority 100)
      (overlay-put ov 'help-echo "Jupyter: executing this region")
      (setq re-ein--running-overlay ov)))
  (re-ein--start-indicator out-buffer))

(defun re-ein--clear-running ()
  "Clear any 'currently executing' indicators."
  (when (and re-ein--running-overlay
             (overlay-buffer re-ein--running-overlay))
    (delete-overlay re-ein--running-overlay))
  (setq re-ein--running-overlay nil)
  (re-ein--stop-indicator))

(defun re-ein-execute (code &optional timeout src-buffer src-start src-end)
  "Send CODE to the Jupyter server and stream output live into the output
buffer. Optional TIMEOUT is the server-side execution cap in seconds.
SRC-BUFFER + SRC-START + SRC-END describe the source region to highlight
while the code is running; nil for code typed at the prompt."
  (interactive
   (if (use-region-p)
       (list (buffer-substring-no-properties (region-beginning) (region-end))
             current-prefix-arg
             (current-buffer)
             (copy-marker (region-beginning))
             (copy-marker (region-end)))
     (list (read-string "Python code: " nil 're-ein-history)
           current-prefix-arg
           nil nil nil)))

  (when (and re-ein-stream-process
             (process-live-p re-ein-stream-process))
    (user-error "A Jupyter execution is already running; M-x re-ein-interrupt to stop"))

  (let* ((payload (encode-coding-string
                   (json-encode `(("code" . ,code)
                                  ("timeout" . ,(or timeout re-ein-timeout))))
                   'utf-8))
         (buffer (get-buffer-create re-ein-output-buffer-name))
         (partial "")
         (pending nil)
         (flush-timer nil)
         (flush-fn
          (lambda ()
            (setq flush-timer nil)
            (when (and pending (buffer-live-p buffer))
              (let ((outputs (nreverse pending)))
                (setq pending nil)
                (with-current-buffer buffer
                  (let ((inhibit-read-only t))
                    (goto-char (point-max))
                    (dolist (output outputs)
                      (re-ein-insert-output output))
                    (when-let ((win (get-buffer-window buffer 0)))
                      (set-window-point win (point-max)))))))))
         (filter
          (lambda (_proc chunk)
            (setq partial (concat partial chunk))
            (let ((lines (split-string partial "\n")))
              (setq partial (car (last lines)))
              (dolist (line (butlast lines))
                (unless (string-empty-p line)
                  (condition-case err
                      (let* ((json-object-type 'alist)
                             (json-array-type 'list)
                             (output (json-read-from-string line)))
                        (push output pending)
                        (unless flush-timer
                          (setq flush-timer
                                (run-at-time 0.15 nil flush-fn))))
                    (error
                     (message "re-ein: bad JSON line: %s (%s)" line err))))))))
         (sentinel
          (lambda (_proc event)
            (when flush-timer
              (cancel-timer flush-timer)
              (setq flush-timer nil))
            (funcall flush-fn)
            (setq re-ein-stream-process nil)
            (re-ein--clear-running)
            (let ((event (string-trim event)))
              (unless (string= event "finished")
                (message "re-ein: %s" event))))))

    (with-current-buffer buffer
      (let ((inhibit-read-only t))
        (erase-buffer)
        (jupyter-output-mode)))
    (display-buffer buffer '((display-buffer-reuse-window
                              display-buffer-pop-up-window)
                             (window-height . 0.4)))

    (re-ein--mark-running buffer src-buffer src-start src-end)

    (setq re-ein-stream-process
          (make-process
           :name "re-ein-stream"
           :buffer nil
           :command (list "curl" "--no-buffer" "-sS" "-N"
                          "-X" "POST"
                          "-H" "Content-Type: application/json"
                          "--data-binary" "@-"
                          (concat re-ein-server-url "/execute/stream"))
           :connection-type 'pipe
           :coding '(utf-8-unix . utf-8-unix)
           :noquery t
           :filter filter
           :sentinel sentinel))
    (process-send-string re-ein-stream-process payload)
    (process-send-eof re-ein-stream-process)))

(defun re-ein-interrupt ()
  "Interrupt the currently running kernel execution (server-side SIGINT)."
  (interactive)
  (let ((url-request-method "POST")
        (url-request-extra-headers '(("Content-Type" . "application/json"))))
    (url-retrieve (concat re-ein-server-url "/kernel/interrupt")
                  (lambda (_status) (message "re-ein: interrupt sent")))))

(defun re-ein-display-output (code response)
  "Display CODE and RESPONSE in the output buffer."
  (let ((buffer (get-buffer-create re-ein-output-buffer-name))
        (outputs (alist-get 'outputs response))
        (status (alist-get 'status response)))

    (with-current-buffer buffer
      (let ((inhibit-read-only t))
        (erase-buffer)
        (jupyter-output-mode)

        ;; Insert header
        ;; (insert "═══════════════════════════════════════════════════════════════\n")
        ;; (insert (propertize "Jupyter Execution Result\n" 'face 'bold))
        ;; (insert (format "Time: %s\n" (format-time-string "%Y-%m-%d %H:%M:%S")))
        ;; (insert (format "Status: %s\n" status))
        ;; (insert "═══════════════════════════════════════════════════════════════\n\n")

        ;; Insert code
        ;; (insert (propertize "Code:\n" 'face 'font-lock-keyword-face))
        ;; (insert "───────────────────────────────────────────────────────────────\n")
        ;; (insert code)
        ;; (insert "\n───────────────────────────────────────────────────────────────\n\n")

        ;; Insert outputs
        ;; (insert (propertize "Output:\n" 'face 'font-lock-keyword-face))
        ;; (insert "───────────────────────────────────────────────────────────────\n")

        (if outputs
            (dolist (output outputs)
              (re-ein-insert-output output))
          (insert "[]"))

        (goto-char (point-min))))

    ;; Display the buffer
    (display-buffer buffer '((display-buffer-reuse-window
                              display-buffer-pop-up-window)
                             (window-height . 0.4)))))

(require 'ansi-color)

(defun re-ein--insert-stream-text (text)
  "Insert TEXT, treating each \\r as 'erase current line and continue'."
  (let ((segments (split-string text "\r")))
    (insert (ansi-color-apply (car segments)))
    (dolist (seg (cdr segments))
      (delete-region (line-beginning-position) (point))
      (insert (ansi-color-apply seg)))))

(defun re-ein-insert-output (output)
  "Insert a single OUTPUT item with appropriate formatting."
  (let ((type (alist-get 'type output)))
    (cond
     ;; Stream output (stdout/stderr) — interpret \r as line-rewrite so
     ;; tqdm-style in-place progress bars update on a single line.
     ((string= type "stream")
      (re-ein--insert-stream-text (alist-get 'text output)))

     ;; Execution result
     ((string= type "execute_result")
      (let ((count (alist-get 'execution_count output))
            (data (alist-get 'data output)))
        ;; (insert (propertize (format "Out[%d]: " count)
        ;;                     'face 'font-lock-function-name-face))
        ;; Check for image first, then fall back to text
        (cond
         ((alist-get 'image/png data)
          (re-ein-insert-image (alist-get 'image/png data)))
         (t
          (let ((raw-text (alist-get 'text/plain data)))
            (insert (or (decode-coding-string raw-text 'utf-8) "[...]")))
          (insert "\n")))))

     ;; Error
     ((string= type "error")
      (let ((ename (alist-get 'ename output))
            (evalue (alist-get 'evalue output))
            (traceback (alist-get 'traceback output)))
        ;; (insert (propertize (format "Error: %s: %s\n" ename evalue)
        ;;                     'face 'error))
        (dolist (line traceback)
          ;; Apply ANSI color codes and insert
          (insert (ansi-color-apply line))
          (unless (string-suffix-p "\n" line)
            (insert "\n")))))

     ;; Display data (e.g., plots)
     ((string= type "display_data")
      (let ((data (alist-get 'data output)))
        ;; (insert (propertize "[Display Data]\n" 'face 'font-lock-keyword-face))
        (cond
         ;; PNG image
         ((alist-get 'image/png data)
          (re-ein-insert-image (alist-get 'image/png data)))
         ;; Text representation
         ((alist-get 'text/plain data)
          (insert (ansi-color-apply (alist-get 'text/plain data)))
          (insert "\n"))
         ;; Other formats
         (t
          (insert (format "  (Data format: %s)\n"
                          (mapconcat 'symbol-name (mapcar 'car data) ", "))))))))))

(defun re-ein-insert-image (base64-data)
  "Insert a PNG image from BASE64-DATA."
  (let* ((image-data (base64-decode-string base64-data))
         (image (create-image image-data 'png t
                              :max-width re-ein-image-max-width)))
    (if image
        (progn
          ;; Optionally save the image
          (when re-ein-save-images
            (re-ein-save-image image-data))
          ;; Insert the image with a text property for easy access
          (let ((start (point)))
            (insert-image image)
            (insert "\n")
            ;; Add properties for interaction
            (add-text-properties start (point)
                                 `(jupyter-image t
                                                 image-data ,image-data
                                                 keymap ,re-ein-image-map))))
      (insert "  (Failed to decode image)\n"))))

(defun re-ein-save-image (image-data)
  "Save IMAGE-DATA to a file in `re-ein-image-directory'."
  (let* ((dir (expand-file-name re-ein-image-directory))
         (filename (format "jupyter-plot-%s.png"
                           (format-time-string "%Y%m%d-%H%M%S")))
         (filepath (expand-file-name filename dir)))
    (unless (file-exists-p dir)
      (make-directory dir t))
    (with-temp-file filepath
      (set-buffer-file-coding-system 'binary)
      (insert image-data))
    (message "Image saved to %s" filepath)))

(defvar re-ein-image-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "RET") 're-ein-view-image-externally)
    (define-key map (kbd "s") 're-ein-save-image-at-point)
    (define-key map (kbd "+") 're-ein-increase-image-size)
    (define-key map (kbd "-") 're-ein-decrease-image-size)
    (define-key map (kbd "=") 're-ein-reset-image-size)
    map)
  "Keymap for interacting with images in Jupyter output.")

(defun re-ein-view-image-externally ()
  "Open the image at point in an external viewer."
  (interactive)
  (let ((image-data (get-text-property (point) 'image-data)))
    (when image-data
      (let ((temp-file (make-temp-file "jupyter-image-" nil ".png")))
        (with-temp-file temp-file
          (set-buffer-file-coding-system 'binary)
          (insert image-data))
        (call-process "open" nil nil nil temp-file)))))

(defun re-ein-save-image-at-point ()
  "Save the image at point to a file."
  (interactive)
  (let ((image-data (get-text-property (point) 'image-data)))
    (if image-data
        (let ((filename (read-file-name "Save image as: "
                                        re-ein-image-directory
                                        nil nil
                                        (format "plot-%s.png"
                                                (format-time-string "%Y%m%d-%H%M%S")))))
          (with-temp-file filename
            (set-buffer-file-coding-system 'binary)
            (insert image-data))
          (message "Image saved to %s" filename))
      (message "No image at point"))))

(defun re-ein-increase-image-size ()
  "Increase the size of the image at point."
  (interactive)
  (re-ein-resize-image-at-point 1.2))

(defun re-ein-decrease-image-size ()
  "Decrease the size of the image at point."
  (interactive)
  (re-ein-resize-image-at-point 0.8))

(defun re-ein-reset-image-size ()
  "Reset the image at point to its original size."
  (interactive)
  (re-ein-resize-image-at-point nil))

(defun re-ein-resize-image-at-point (factor)
  "Resize the image at point by FACTOR. If FACTOR is nil, reset to original."
  (let ((image-data (get-text-property (point) 'image-data))
        (inhibit-read-only t))
    (when image-data
      (let* ((start (previous-single-property-change (point) 'jupyter-image))
             (end (next-single-property-change (point) 'jupyter-image))
             (image-display (get-text-property (point) 'display)))
        (when (and start end image-display)
          (delete-region start end)
          (goto-char start)
          (let ((new-image (if factor
                               (create-image image-data 'png t
                                             :scale factor)
                             (create-image image-data 'png t
                                           :max-width re-ein-image-max-width))))
            (insert-image new-image)
            (insert "\n")
            (add-text-properties start (point)
                                 `(jupyter-image t
                                                 image-data ,image-data
                                                 keymap ,re-ein-image-map))))))))

(defun re-ein-execute-region (start end)
  "Execute the region between START and END."
  (interactive "r")
  (re-ein-execute (buffer-substring-no-properties start end)
                       nil (current-buffer)
                       (copy-marker start) (copy-marker end)))

(defun re-ein-execute-buffer ()
  "Execute the entire buffer."
  (interactive)
  (re-ein-execute (buffer-string)
                       nil (current-buffer)
                       (copy-marker (point-min))
                       (copy-marker (point-max))))

(defun re-ein-execute-paragraph ()
  "Execute the current paragraph."
  (interactive)
  (save-excursion
    (let ((start (progn (backward-paragraph) (point)))
          (end (progn (forward-paragraph) (point))))
      (re-ein-execute (buffer-substring-no-properties start end)
                           nil (current-buffer)
                           (copy-marker start) (copy-marker end)))))

(defun re-ein-execute-line ()
  "Execute the current line."
  (interactive)
  (re-ein-execute (thing-at-point 'line t)
                       nil (current-buffer)
                       (copy-marker (line-beginning-position))
                       (copy-marker (line-end-position))))

(defun re-ein-kernel-status ()
  "Check the kernel status."
  (interactive)
  (let ((url-request-method "GET")
        (response-buffer
         (url-retrieve-synchronously
          (concat re-ein-server-url "/kernel/status")
          nil nil 5)))
    (if response-buffer
        (with-current-buffer response-buffer
          (goto-char (point-min))
          (re-search-forward "^$" nil 'move)
          (forward-char)
          (let* ((json-object-type 'alist)
                 (response (json-read)))
            (kill-buffer response-buffer)
            (message "Kernel status: %s" (alist-get 'status response))))
      (message "Failed to connect to Jupyter server"))))

(defun re-ein-restart-kernel ()
  "Restart the Jupyter kernel."
  (interactive)
  (when (yes-or-no-p "Restart Jupyter kernel? This will clear all variables. ")
    (let* ((url-request-method "POST")
           (url-request-extra-headers
            '(("Content-Type" . "application/json")))
           (response-buffer
            (url-retrieve-synchronously
             (concat re-ein-server-url "/kernel/restart")
             nil nil 10)))
      (if response-buffer
          (progn
            (kill-buffer response-buffer)
            (message "Kernel restarted successfully"))
        (message "Failed to restart kernel")))))

;; Define a simple major mode for the output buffer
(define-derived-mode jupyter-output-mode special-mode "Jupyter Output"
  "Major mode for displaying Jupyter execution output.

Image interaction keys when cursor is on an image:
\\{re-ein-image-map}"
  (setq buffer-read-only t))

(provide 're-ein)

;;; re-ein.el ends here
