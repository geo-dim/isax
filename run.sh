make clean
make


#rm ../../data.bin.*
#./bin/ads-full --algorithm 0  --dataset /home/geodim/Desktop/src.haris/random-data-generator/data.bin --dataset-size 100 --timeseries-size 256 --leaf-size 10 --flush-limit 100
#./bin/ads-full --queries ../../data.bin --queries-size 100 --index-path myexperiment --use-index



#rm ../../data.bin.*
#rm ../../data1M_a.bin.*
#rm ../../data1M_b.bin.*
#rm ../../data10M_a.bin.*
#valgrind --leak-check=full --show-reachable=yes 
./bin/ads-full --algorithm 0 --dataset /home/geodim/Desktop/src.haris/random-data-generator/data.bin --dataset-size 1000 --timeseries-size 256 --leaf-size 100 --flush-limit 1000
#./bin/ads-full --algorithm 0 --dataset ../../data10M_a.bin --dataset-size 10000000 --timeseries-size 256 --leaf-size 100 --flush-limit 100000

#rm ../../data10M_a.bin.*
#./bin/ads-full --algorithm 0 --dataset ../../data10M_a.bin --dataset-size 10000000 --timeseries-size 256 --leaf-size 100 --flush-limit 200000

#rm ../../data10M_a.bin.*
#./bin/ads-full --algorithm 0 --dataset ../../data10M_a.bin --dataset-size 10000000 --timeseries-size 256 --leaf-size 100 --flush-limit 500000

#valgrind --leak-check=yes ./bin/ads-full --algorithm 0 --dataset ../../data1M_a.bin --dataset-size 1000000 --timeseries-size 256 --leaf-size 100 --flush-limit 10000
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index


#./bin/ads-full --queries ../../data10M_a.bin --queries-size 100 --index-path myexperiment --use-index
 
#rm ../../data10M_a.bin.*
#./bin/ads-full --algorithm 0  --dataset ../../data10M_a.bin --dataset-size 10000000 --timeseries-size 256 --leaf-size 100 --flush-limit 10000000
#./bin/ads-full --queries ../../data10M_a.bin --queries-size 1000 --index-path myexperiment --use-index
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 1000 --index-path myexperiment --use-index


#./bin/ads-full --queries ../../data1M.bin --queries-size 100 --index-path myexperiment --use-index
#/./bin/ads-full --algorithm 0 --dataset ../../data10M.bin --dataset-size 10000000 --timeseries-size 256 --leaf-size 100 --flush-limit 10000000

#rm ../../data1M.bin.*
#./bin/ads-full --algorithm 0  --dataset ../../data1M.bin --dataset-size 1000000 --timeseries-size 256 --leaf-size 100 --flush-limit 1000000 
#./bin/ads-full --queries ../../data10M_b.bin --queries-size 100 --index-path myexperiment --use-index

#./bin/ads-full --algorithm 0  --dataset ../../data1M.bin --dataset-size 1000000 --timeseries-size 256 --leaf-size 100 --flush-limit 200000 > result2 


#./bin/ads-full --algorithm 3  --dataset ../../data1M.bin --dataset-size 1000000 --timeseries-size 256 --leaf-size 10 --flush-limit 200000
#./bin/ads-full --algorithm 5  --dataset ../../data1M.bin --dataset-size 1000000 --timeseries-size 256 --leaf-size 10 --flush-limit 200000
#./bin/ads-full --queries ../../data1M.bin --queries-size 1000000 --index-path myexperiment --use-index



