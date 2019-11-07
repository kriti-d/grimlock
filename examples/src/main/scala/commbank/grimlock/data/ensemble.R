num.points <- 500

x1 <- rnorm(num.points, 1, 1.5)
y1 <- rnorm(num.points, 1, 1.5)
z1 <- rnorm(num.points, 1, 1.5)
l1 <- rep(1, num.points)

x0 <- rnorm(num.points, -1, 2)
y0 <- rnorm(num.points, -1, 2.5)
z0 <- rnorm(num.points, -1, 1)
l0 <- rep(0, num.points)

plot(x0, y0, xlim=range(c(x0, x1)), ylim=range(c(y0, y1)), col='red')
lines(x1, y1, col='blue', type='p')

plot(x0, z0, xlim=range(c(x0, x1)), ylim=range(c(z0, z1)), col='red')
lines(x1, z1, col='blue', type='p')

plot(y0, z0, xlim=range(c(y0, y1)), ylim=range(c(z0, z1)), col='red')
lines(y1, z1, col='blue', type='p')

ord <- order(c(ord1 <- 2 * (1:num.points) - 1, 2 * (1:num.points)))

df <- data.frame(entity=sapply((1:(2 * num.points)), function(i) paste0('iid:', i)),
                 label=c(l1, l0)[ord],
                 x=c(x1, x0)[ord], y=c(y1, y0)[ord], z=c(z1, z0)[ord])

write.table(df, file='exampleEnsemble.txt', quote=F, row.names=F, col.names=F, sep="|")

