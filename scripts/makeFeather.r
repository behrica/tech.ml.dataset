#install.packages(c("arrow","feather"),repos = "http://cran.us.r-project.org")


data <- data.frame(x1 = c(1, 5, 8, 2),
                   x2 = c(3, 2, 5, 2),
                   x3 = c(2, 7, 1, 2))
data$x1 <- as.factor(data$x1)
data$x2 <- as.character(data$x2)
data$x3 <- as.integer(data$x3)
data$x4 <- as.numeric(c(8.0,9.0,10.0,11.0))
data$x5 <- as.POSIXct("1/1/2020")
data$x6 <- c(T,F)
data$x7 <- c(as.Date("1970-01-01"))

arrow::write_feather(data,"/tmp/data.feather_arrow")
feather::write_feather(data,"/tmp/data.feather_feather")
arrow::write_ipc_stream(data,"/tmp/data.ipc_stream")
arrow::write_arrow(data,"/tmp/data.arrow") # same as arrow::write_feather
