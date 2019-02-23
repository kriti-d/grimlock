#!/usr/bin/env Rscript

library(reshape2)

# ensure repeatable results
set.seed(1234)

# read data from standard in
df <- read.table(file("stdin"), sep='|', stringsAsFactors=F,
                 col.names=c('entity.id', 'feature', 'hash', 'encoding', 'type', 'value'))

# get unique entity id with hash code, this is used to split the data into train test
hash <- unique(df[, c("entity.id", "hash")])

# remove columns that are no longer needed
df$type <- NULL
df$encoding <- NULL
df$hash <- NULL

# convert data from long to wide format
data <- dcast(df, entity.id ~ feature)

# split the data into train and test set
train <- data[data$entity.id %in% (hash$entity.id[hash$hash < 8]), ]
test <- data[data$entity.id %in% (hash$entity.id[hash$hash >= 8]), ]

# build model and generate scores.
#
# Note: This is not representative. Normally one would do proper model tuning
#       and validation before scoring the test set. This merely serves as a
#       simple example.
model <- glm(label ~ x + y + z, data=train, family=binomial())
scores <- predict(model, test, type="response")

# generate output data and write the result to standard out (to be read my grimlock library)
output <- data.frame(entity.id=test$entity.id, encoding='double', type='continuous', lr=scores)
write.table(output, file="", sep="|", quote=F, row.names=F)

