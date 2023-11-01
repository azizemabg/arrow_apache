################################################################################
################################################################################

rm(list = ls())

getwd()

install.packages("doSNOW")
install.packages("microbenchmark")
install.packages("foreach")
install.packages("doParallel")


library(doSNOW)
library(tidyverse)
library(microbenchmark)
library(foreach)
library(furrr)
library(parallel)
library(data.table)
library(doParallel)


# Example of using microbenchmark
library(microbenchmark)
library(datasets)


# Define the code to be measured
my_code <- function() {
  for (i in 1:1000) {
    x <- rnorm(1000)
    y <- rnorm(1000)
    z <- x + y
  }
}

# Measure the execution time of the code
mb <- microbenchmark(my_code())
print(mb)

data(package = "dplyr")
data(package = "tidyr")
data(package = "ggplot2")
data(package = "datasets")

# I am going to use mpg data from ggplor2
diamonds.data <- diamonds

# Settting the cores for parallel computing
?makeCluster
cl <- makeCluster(6, type = "SOCK") # 4 number of cores
registerDoSNOW(cl)
stopCluster(cl) # undo the parallel processing setup

# test the speed
test1 <- microbenchmark(foreach(i = 1:8000) %dopar% {sqrt(i)}) # example 1
print(test1)

test2 <- microbenchmark(foreach(i = 1:8000) %do% {sqrt(i)}) # example 2
print(test2)

makeCluster(detectCores())

# METHOD 2
head(diamonds.data)

# set number of thread to use for parallel computing
setDTthreads(6)

cl <- makeCluster(detectCores())
registerDoParallel(cl)

55000/10

write_csv(diamonds.data, file = "diamonds.data.csv")


library(data.table)
library(parallel)

# Set the number of cores to use
num_cores <- detectCores()

diamonds.data.par <- fread("diamonds.data.csv", header = TRUE, sep = ",", 
                 data.table = FALSE, verbose = TRUE, 
                 showProgress = TRUE, 
                 nThread = num_cores)
microbenchmark(diamonds.data.par)

diamonds.data <- fread("diamonds.data.csv", header = TRUE, sep = ",", 
                       data.table = FALSE)
microbenchmark(diamonds.data)
names(diamonds.data.par)

diamonds.data.par <- rename(diamonds.data.par, potongan = cut, 
                  karat = carat, 
                  .parallel = TRUE, 
                  .packages = "tidyverse")

num_cores <- detectCores()

diamonds.data.par <- rename(diamonds.data.par, 
                            potongan = cut, 
                            karat = carat, 
                            .parallel = TRUE, 
                            .packages = "tidyverse",
                            check.names = FALSE)
?data.table

################################################################################
###############################################################################
#
#
#
#
################################################################################
################################################################################

## An example

# RENAMING THE COLUMNS HEADERS ####
# Create a sample data.table
dt <- data.table(A = 1:5, B = 6:10, C = 11:15)
dt

# Rename the column headers
setnames(dt, old = c("A", "B", "C"), new = c("Column1", "Column2", "Column3"))
dt


# Converting class function
lapply(rawdata.tidy, function(x) class(x))


# Change class to factor for column A
setattr(dt$A, "class", "factor")
dt

?setattr

# converting class variable in multiple call
set(dt, j = "Category", value = as.factor(dt$Category))
set(dt, j = "Value1", value = as.numeric(dt$Value1))
set(dt, j = "Value2", value = as.numeric(dt$Value2))

# Converting class variable in single call
dt[, c("Category", "Value1", "Value2") := .(as.character(Category), as.numeric(Value1), as.numeric(Value2))]


# grouping and summarising
# Create a sample data.table
dt <- data.table(Category = c("A", "A", "B", "B", "B"),
                 Status = c("Rich", "Poor", "Poor", "Rich", "Poor"),
                 Value1 = c(10, 20, 15, 25, 30),
                 Value2 = c(5, 8, 6, 9, 12))
dt


# Summarise
# Group by "Category" and calculate the sum of "Value1" and "Value2"
dt_summary <- dt[, .(Sum_Value1 = sum(Value1), Sum_Value2 = sum(Value2)), by = .(Category, Status)][order(-Category)]
dt_summary

###############################################################################
###############################################################################

# Load required packages
library(parallel)
library(foreach)
library(doParallel)
library(tidyverse)

# Set up parallel backend
cl <- makeCluster(detectCores())
registerDoParallel(cl)

# Load data
data(mtcars)

# Filter and summarize data in parallel
mtcars %>%
  as_tibble() %>%
  filter(cyl == 4) %>%
  group_by(gear) %>%
  summarize(mean_hp = mean(hp)) %>%
  collect()

# Stop parallel backend
stopCluster(cl)

#Alternative to improve the speed from hadley wickham packages
library(multidplyr)
library(dtplyr)


vignette("multidplyr")
vignette("translation")

###############################################################################
###############################################################################
###############################################################################

# APPLY data.table in single milking ams data (rawdata.tidy.csv) ####

rawdata.tidy <- fread("rawdata.tidy.csv", stringsAsFactors = TRUE)
names(rawdata.tidy)
lapply(rawdata.tidy, function(x) class(x))


setnames(dt, old = c("A", "B", "C"), new = c("Column1", "Column2", "Column3"))

# renaming data (WORKS)
setnames(rawdata.tidy, 
         old = c("farm", "animal", "age_year", "milking_date", "year", 
                 "month", "day", "milking_hours", "breed", "date_of_birth", 
                 "current_age", "lactation", "calving_date", "next_calving_date", "lact_end",
                 "lactend_date", "lactend_time", "lag_timestamp", "lag_date", "lag_time",
                 "timestamp", "timestamp_date", "timestamp_time", "lead_timestamp", "lead_date",
                 "lead_time", "milk_kg", "milk_kg_hour", "avg_milk_hour", "milk_interval", 
                 "milk_start", "milkstart_date", "milkstart_time", "duration", "val_status"), 
         new = c("kandang", "hewan", "umur_th", "tgl_peras", "tahun", 
                 "bulan", "hari", "jam_peras", "ras", "tgl_lahir", 
                 "umur_sekarang", "laktasi", "tgl_melahirkan", "melahirkan_selanjutnya", "akhir_laktasi", 
                 "tgl_akhirlaktasi", "jam_akhirlaktasi", "prior_perah", "prior_tglperah", "prior_jamperah",
                 "perah", "tgl_perah", "jam_perah", "perah_selanjutnya", "tgl_perahselanjutnya",
                 "jam_perahselanjutnya", "susu_kg", "susu_kg_jam", "rata_susu_jam", "interval_susu", 
                 "mulai_susu", "tgl_mulaisusu", "jam_mulaisusu", "durasi", "status"))
names(rawdata.tidy)


# Change class (WORKED)
rawdata.tidy[, c("kandang", "hewan", "umur_th", "tgl_peras", "tahun", 
                 "bulan", "hari", "jam_peras", "ras", "tgl_lahir", 
                 "umur_sekarang", "laktasi", "tgl_melahirkan", "melahirkan_selanjutnya", "akhir_laktasi", 
                 "tgl_akhirlaktasi", "jam_akhirlaktasi", "prior_perah", "prior_tglperah", "prior_jamperah",
                 "perah", "tgl_perah", "jam_perah", "perah_selanjutnya", "tgl_perahselanjutnya",
                 "jam_perahselanjutnya", "susu_kg", "susu_kg_jam", "rata_susu_jam", "interval_susu", 
                 "mulai_susu", "tgl_mulaisusu", "jam_mulaisusu", "durasi", "status") := .(as.factor(kandang), as.factor(hewan), as.factor(umur_th), as.POSIXct(tgl_peras, format = "%Y-%m-%d %H:%M:%S"), as.factor(tahun), 
                                                                                          as.integer(bulan), as.integer(hari), as.POSIXct(jam_peras, format = "%H:%M:%S"), as.factor(ras), as.POSIXct(tgl_lahir, format = "%Y-%m-%d"), 
                                                                                          as.factor(umur_sekarang), as.factor(laktasi), as.POSIXct(tgl_melahirkan, format = "%Y-%m-%d"), as.POSIXct(melahirkan_selanjutnya, format = "%Y-%m-%d"), as.POSIXct(akhir_laktasi, format = "%Y-%m-%d"), 
                                                                                          as.POSIXct(tgl_akhirlaktasi, format = "%Y-%m-%d"), as.POSIXct(jam_akhirlaktasi, format = "%Y-%m-%d %H:%M:%S"), as.POSIXct(prior_perah, format = "%Y-%m-%d %H:%M:%S"), as.POSIXct(prior_tglperah, format = "%Y-%m-%d"), as.POSIXct(prior_jamperah, format = "%Y-%m-%d %H:%M:%S"),
                                                                                          as.POSIXct(perah, format = "%Y-%m-%d %H:%M:%S"), as.POSIXct(tgl_perah, format = "%Y-%m-%d %H:%M:%S"), as.POSIXct(jam_perah, format = "%H:%M:%S"), as.POSIXct(perah_selanjutnya, format = "%Y-%m-%d"), as.POSIXct(tgl_perahselanjutnya, format = "%Y-%m-%d"), 
                                                                                          as.POSIXct(jam_perahselanjutnya, format = "%Y-%m-%d %H:%M:%S"), as.numeric(susu_kg), as.numeric(susu_kg_jam), as.numeric(rata_susu_jam), as.numeric(interval_susu), 
                                                                                          as.POSIXct(mulai_susu, format = "%Y-%m-%d %H:%M:%S"), as.POSIXct(tgl_mulaisusu, format = "%Y-%m-%d"), as.POSIXct(jam_mulaisusu, format = "%Y-%m-%d %H:%M:%S"), as.numeric(durasi), as.character(status))]
lapply(rawdata.tidy, function(x) class(x))

# Grupby and summarise
n_visit_animal <- rawdata.tidy[, .(n_visit = length(tgl_perah),
                                   totmilk_kg = sum(susu_kg),
                                   avg_milk_hour = mean(susu_kg_jam),
                                   totinterval_min = sum(interval_susu),
                                   avg_interval_min = mean(interval_susu),
                                   totduration_min = sum(durasi, na.rm = TRUE),
                                   avg_duration_min = mean(durasi, na.rm = TRUE)),
                               by = .(hewan, tgl_peras)][order(-tgl_peras)]

rm(n_visit_animal)
test <- n_visit_animal %>% 
  filter(hewan == "0129dc733fd270486b548671e9354623")


