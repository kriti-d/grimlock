#!/usr/bin/env Rscript

labels <- read.table(file("../data/exampleEnsemble.txt"), sep='|', stringsAsFactors=F,
                     col.names=c('entity.id', 'label', 'x', 'y', 'z'))

labels$x <- NULL
labels$y <- NULL
labels$z <- NULL

# cat ./demo.spark/ensemble.scores/part* > scores.txt
scores <- read.table(file("./scores.txt"), sep='|', stringsAsFactors=F,
                     col.names=c('entity.id', 'type', 'encoding', 'score'))

scores$type <- NULL
scores$encoding <- NULL

df <- merge(labels, scores, by='entity.id')

gini <- function(y) {
  tpr <- cumsum(y == 1) / sum(y == 1)
  fpr <- cumsum(y == 0) / sum(y == 0)
  1 - as.numeric((tpr[-1] + tpr[1:(length(tpr) - 1)]) %*% (fpr[-1] - fpr[1:(length(fpr) - 1)]))
}

gini(df$label[order(df$score)])

