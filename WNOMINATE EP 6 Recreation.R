pacman::p_load(wnominate,dplyr,pscl,ggplot2,here,remotes,pkgbuild,emIRT)
here()
mepsEP6<-read.csv(here("Cleaned_data","EP6_clean_data","mep_info_for_wnominate.csv"),header=TRUE,strip.white=TRUE)
votesEP6 <- read.csv("./Cleaned_data/EP6_clean_data/wnominate_ep6_votes.csv",header = TRUE, strip.white = TRUE)
pkgbuild::check_build_tools(debug = TRUE)
install_github("kosukeimai/emIRT")

names <- mepsEP6[,1]
legData <- matrix(mepsEP6[,2],length(mepsEP6[,2]),1)
colnames(legData) <- "EPG"
matrix_df <- as.data.frame(votesEP6)
rc2 <- rollcall(votesEP6, yea = 1 , nay = 2 , missing = 3,notInLegis = 0,legis.names=names,legis.data = legData,desc="EP6")
result<-wnominate(rc2,polarity=c(10,10))
summary(result) 
legislators <- data.frame(
  EPG = result$legislators$EPG,
  coord1D = result$legislators$coord1D,
  coord2D = result$legislators$coord2D
)
ggplot(legislators, aes(x = coord1D, y = coord2D, color = EPG, label = EPG)) +
  geom_point(size = 3) +
  geom_text(vjust = 1.5, hjust = 1.5, check_overlap = TRUE) +
  labs(title = "Legislators on Cartesian Plane",
       x = "Coordinate 1D",
       y = "Coordinate 2D") +
  theme_minimal() +
  scale_color_discrete(name = "EPG Labels")
result1d <- wnominate(rc2,dims = 1, polarity = c(10))

legislators2 <- data.frame (
  EPG = result1d$legislators$EPG,
  coord1D = result1d$legislators$coord1D,
  
)
# Create a constant y-value
legislators2$y <- 0

# Plotting with ggplot2
ggplot(legislators2, aes(x = coord1D, y = y, color = EPG, label = EPG)) +
  geom_point(size = 3) +
  geom_text(vjust = 1.5, hjust = 1.5, check_overlap = TRUE) +
  labs(title = "1 dimension WNOMINATE",
       x = "Coordinate 1D",
       y = "") +
  theme_minimal() +
  scale_color_discrete(name = "EPG Labels") +
  xlim(-1, 1) +
  theme(axis.text.y = element_blank(), 
        axis.ticks.y = element_blank(), 
        axis.title.y = element_blank(),
        panel.grid.major.y = element_blank(),
        panel.grid.minor.y = element_blank())

Ideal_points_ep6 <- data.frame (
  MepId <- mepsEP6$MepId,
  EPG <- mepsEP6$EPG,
  correctYea <- result1d$legislators$correctYea,
  wrongYea <- result1d$legislators$wrongNay,
  correctNay <- result1d$legislators$correctNay,
  wrongNay <- result1d$legislators$wrongNay,
  GMP <- result1d$legislators$GMP,
  CC <- result1d$legislators$CC,
  coord1D <- result1d$legislators$coord1D
)
write.csv(Ideal_points_ep6,file = here("Results","EP6_1D_Ideal_points_WNOMINATE.csv"),row.names = FALSE)
data(AsahiTodai)
data(s109)
rcEM <- convertRC(rc2)
p <- makePriors(rcEM$n, rcEM$m, 1)
s <- getStarts(rcEM$n, rcEM$m, 1)
resultEM <- binIRT(.rc = rcEM,
                   .starts = s,
                   .priors = p,
                   )
legislators3 <- data.frame(
  EPG <- EPG,
  coord1d <- resultEM$means$x
)
legislators3$y <- 0

ggplot(legislators3, aes(x = d1, y = y, color = EPG, label = EPG)) +
  geom_point(size = 3) +
  geom_text(vjust = 1.5, hjust = 1.5, check_overlap = TRUE) +
  labs(title = "1 dimension emIRT",
       x = "Coordinate 1D",
       y = "") +
  theme_minimal() +
  scale_color_discrete(name = "EPG Labels") +
  xlim(-10, 10) +
  theme(axis.text.y = element_blank(), 
        axis.ticks.y = element_blank(), 
        axis.title.y = element_blank(),
        panel.grid.major.y = element_blank(),
        panel.grid.minor.y = element_blank())
normalize <- function(x) {
  return((2 * (x - min(x)) / (max(x) - min(x))) - 1)
}
legislators3normalized <- legislators3
legislators3normalized$d1 <- normalize(legislators3$d1)
ggplot(legislators3normalized, aes(x = d1, y = y, color = EPG, label = EPG)) +
  geom_point(size = 3) +
  geom_text(vjust = 1.5, hjust = 1.5, check_overlap = TRUE) +
  labs(title = "1 dimension emIRT normalized to -1,1",
       x = "Coordinate 1D",
       y = "") +
  theme_minimal() +
  scale_color_discrete(name = "EPG Labels") +
  xlim(-1, 1) +
  theme(axis.text.y = element_blank(), 
        axis.ticks.y = element_blank(), 
        axis.title.y = element_blank(),
        panel.grid.major.y = element_blank(),
        panel.grid.minor.y = element_blank())
resultEMcentered <- binIRT(.rc = rcEM,
                   .starts = s,
                   .priors = p,
                   .anchor_subject = 10
)
legislators4 <- data.frame(
  EPG <- EPG,
  coord1d <- resultEMcentered$means$x
)
legislators4$y <- 0
legislators4normalized <- legislators4
legislators4normalized$d1 <- normalize(legislators4$d1)

ggplot(legislators4, aes(x = d1, y = y, color = EPG, label = EPG)) +
  geom_point(size = 3) +
  geom_text(vjust = 1.5, hjust = 1.5, check_overlap = TRUE) +
  labs(title = "1 dimension emIRT centered",
       x = "Coordinate 1D",
       y = "") +
  theme_minimal() +
  scale_color_discrete(name = "EPG Labels") +
  xlim(-10, 10) +
  theme(axis.text.y = element_blank(), 
        axis.ticks.y = element_blank(), 
        axis.title.y = element_blank(),
        panel.grid.major.y = element_blank(),
        panel.grid.minor.y = element_blank())

ggplot(legislators4normalized, aes(x = d1, y = y, color = EPG, label = EPG)) +
  geom_point(size = 3) +
  geom_text(vjust = 1.5, hjust = 1.5, check_overlap = TRUE) +
  labs(title = "1 dimension emIRT centered normalized to -1,1",
       x = "Coordinate 1D",
       y = "") +
  theme_minimal() +
  scale_color_discrete(name = "EPG Labels") +
  xlim(-1, 1) +
  theme(axis.text.y = element_blank(), 
        axis.ticks.y = element_blank(), 
        axis.title.y = element_blank(),
        panel.grid.major.y = element_blank(),
        panel.grid.minor.y = element_blank())
resultEMcentered <- binIRT(.rc = rcEM,
                           .starts = s,
                           .priors = p,
                           .anchor_subject = 10
)