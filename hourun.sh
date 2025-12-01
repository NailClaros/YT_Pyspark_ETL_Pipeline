#!/bin/bash
# hourun.sh

# Exit immediately if a command exits with a non-zero status
set -e

RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
BLUE="\e[34m"
MAGENTA="\e[35m"
CYAN="\e[36m"
BOLD="\e[1m"
RESET="\e[0m"


SECONDS=0


echo -e "${BLUE}${BOLD}🚀 Running spark_pipeline.py...${RESET}"
python spark_pipeline.py
echo -e "${GREEN}${BOLD}✅ Finished spark_pipeline.py in ${SECONDS}s${RESET}"


echo -e "${BLUE}${BOLD}🚀 Running etl_spark.py...${RESET}"
python etl_spark.py
echo -e "${GREEN}${BOLD}✅ Finished etl_spark.py in ${SECONDS}s${RESET}"


echo -e "${MAGENTA}${BOLD}🎉 All scripts completed successfully in ${SECONDS}s!${RESET}"