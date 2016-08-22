f <- file("stdin")
open(f)
while(length(line <- readLines(f, n=1)) > 0) {
  parts <- strsplit(line, "[|]")[[1]]

  write(paste(c(parts[1], parts[4], parts[2], parts[3], as.numeric(parts[4]) * 2), collapse="#"), stdout())
}
