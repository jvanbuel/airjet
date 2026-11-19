;; Busy-loops forever: used to demo timeouts and cancellation (UI "mark failed").
(module
  (memory (export "memory") 1)
  (func (export "_start") (loop (br 0))))
