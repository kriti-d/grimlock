library(reshape2)

df <- melt(HairEyeColor)

df$id <- paste0('rid:', 1:nrow(df))
df$hair <- tolower(as.character(df$Hair))
df$eye <- tolower(as.character(df$Eye))
df$gender <- tolower(as.character(df$Sex))

df$Hair <- NULL
df$Eye <- NULL
df$Sex <- NULL

df <- melt(df, id.vars = 'id', measure.vars = c('hair', 'eye', 'gender', 'value'))

df$type <- sapply(df$variable, function(v) ifelse(v == 'value', 'discrete', 'nominal'))
df$encoding <- sapply(df$variable, function(v) ifelse(v == 'value', 'long', 'string'))

write.table(df[, c('id', 'variable', 'encoding', 'type', 'value')],
            file="exampleConditional.txt", quote=F, sep='|', row.names=F, col.names=F)
