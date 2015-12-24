
_() {
	echo "scale = 3; $@" | bc
}

for n in 12 14 16 18; do
	for d in `seq 1 8`; do
		echo -n "n=$n d=$d t="
		_ \( 0 $(for i in `seq 10`; do
			echo +
			sbt "run $d $d" | grep -v '\[' | head -1
		done) \) / 10
	done
done

