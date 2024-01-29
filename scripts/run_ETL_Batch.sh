#!/bin/bash
filename=`basename "$0" .sh`

usage() {
cat <<EOF
  run_ETL_Batch.sh Usage:

        -s : Step to run (1,2,3)  (mandatory)
        -c : Cluster env (allowed values: qa, dv, pr)  (mandatory)

Syntax : bash run_ETL_Batch.sh -s 1 -c dv

EOF
}

while getopts "s:c:" OPTION
do
    case $OPTION in
        s) STEP=$OPTARG;;
        c) ENV_CLUSTER=$OPTARG;;
    esac
done

# Check required parameters
if [[ -z ${STEP} ]] || [[ -z ${ENV_CLUSTER} ]];
then
  echo "****************************************************"
  echo "Options  -s and -c  can not be empty"
  echo "****************************************************"
     usage
     exit 1
fi

PROFILE_LOCATION="/path/to/HDFS/profile"
JAR_LOCATION="/path/to/HDFS/jar"
LOGS_LOCATION="/path/to/HDFS/logs"

DRIVER_CLASS=("ETL_Batch_Process")
DRIVER_CLASS_FULL_NAME="com.norfolk.${DRIVER_CLASS}"
PROFILE_NAME="app_config_$ENV_CLUSTER.config"
JAR_FILE="ETL_Batch_Process-SNAPSHOT.jar"
MODULE_NAME="sample_module_1"

CUR_DIR=`pwd`

export LOG_FILE=$CUR_DIR/logs/${filename}_$(date +"%Y%m%d_%H%M%S")_$STEP.log

LOG_FOLDER=$(dirname "${LOG_FILE}")
if [[ ! -e ${LOG_FOLDER} ]]; then
  #Creating the log directory if does not exist
        echo "Log path not found, Creating log Folder: ${LOG_FOLDER}"
        mkdir -p ${LOG_FOLDER}
fi

exec 1> $LOG_FILE 2>&1

CONF_FILE=$CUR_DIR/$PROFILE_NAME

echo "***************Printing Parameters******************"
echo "CONF_FILE=$CONF_FILE"
echo "CUR_DIR=$CUR_DIR"
echo "****************************************************"

SPARK_SUBMIT=/usr/bin/spark-submit

echo "***** $MODULE_NAME started at: $(date +%x_%r) *****"

  SPARK_SUB_COMMAND="$SPARK_SUBMIT --master yarn --deploy-mode cluster --name $MODULE_NAME --executor-cores 2 --num-executors 50 --executor-memory 60g --driver-memory 25g --files $PROFILE_LOCATION/$PROFILE_NAME --class ${DRIVER_CLASS_FULL_NAME} $JAR_LOCATION/$JAR_FILE $PROFILE_NAME $STEP"

echo $SPARK_SUB_COMMAND

${SPARK_SUB_COMMAND} &>> ${LOG_FILE}

if [ $? -eq 0 ]
    then
      echo "***** Finished $MODULE_NAME successfully at: $(date +%x_%r) *****"
      echo "Finished $MODULE_NAME successfully. Logs are available at $CUR_DIR/logs"
      aws s3 cp $LOG_FILE $S3_LOGS_LOCATION/$MODULE_NAME/
    else
      echo "***** Error in $MODULE_NAME execution at: $(date +%x_%r) *****"
      echo "Error in $MODULE_NAME  execution. Logs are available at $CUR_DIR/logs"
      aws s3 cp $LOG_FILE $S3_LOGS_LOCATION/$MODULE_NAME/
      exit 1
fi

exit $?