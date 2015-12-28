
_() {
	echo "scale = 3; $@" | bc
}

avg() {
	_ \( 0 $(for i in $@; do
		echo + $i
	done) \) / $#
}

TM=1 # time
WL=2 # rec calls
SC=3 # scheds

get_line() {
	sed "${1}q;d"
}

get_value() {
	sbt "run $1 $2" | grep -v '\[' | get_line $3
}

plot_workload_by_depth_fixed_N() {
	N=22
	for d in `seq 1 $N`; do
		echo -n "$N $d "
		avg $(for i in `seq 5`; do get_value $N $d $WL; done)
	done
}

scheds_by_depth_fixed_N() {
	N=18
	for d in `seq 1 $N`; do
		echo -n "$N $d "
		avg $(for i in `seq 3`; do get_value $N $d $SC; done)
	done
}

time_by_depth_and_N() {
	for n in 12 14 16 18; do
		for d in `seq 2 2 12`; do
			echo -n "n=$n d=$d t="
			_ \( 0 $(for i in `seq 3`; do
				echo +
				sbt "run $d $d" | grep -v '\[' | head -1
			done) \) / 3
		done
	done
}

scheds_by_depth_fixed_N

