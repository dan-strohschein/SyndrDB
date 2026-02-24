curl -s "http://localhost:6060/debug/pprof/mutex" -o mutex_150.prof
 curl -s "http://localhost:6060/debug/pprof/block" -o block_150.prof
 curl -s "http://localhost:6060/debug/pprof/goroutine?debug=2" -o goroutine_150.txt
 curl -s "http://localhost:6060/debug/pprof/heap" -o heap_150.prof