if [ "$#" -ne 4 ] 
	then
    	echo 'Syntaxe: ./script.sh [USERNAME] [BLOCK_SIZE_VALUE] [START_FROM] [STOP_AT]'
    	echo 'Example: ./script.sh user 4m 1901 1902'
    	exit 0
	fi
APP_USER="/user/$1";
echo $APP_USER
APP_HOME=$APP_USER"/weather";
echo $APP_HOME

APP_DATA=$APP_HOME"/data";
echo $APP_DATA
hdfs dfs -mkdir -p $APP_DATA;

BLOCK_SIZE_VALUE="$2";

let START_FROM=$3
let STOP_AT=$4
for (( i = "$START_FROM"; i <= "$STOP_AT"; i++ )); do
	hadoop fs -mkdir $APP_DATA/$i;
done
for (( i = "$START_FROM"; i < "$STOP_AT"; i++ )); do
    mkdir "$i";
    wget -O "$i".tar.gz "https://www.ncei.noaa.gov/data/global-hourly/archive/csv/${i}.tar.gz";
    tar -zxvf "$i".tar.gz -C "$i";
    hdfs dfs -D dfs.blocksize=$BLOCK_SIZE_VALUE -put $i $APP_DATA
done
