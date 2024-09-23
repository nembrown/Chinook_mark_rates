ggplot(Sport_mark_rate_only_by_year %>% filter(AREA=="Area 19 (JDF)", YEAR==2019), aes(y=mark_rate, x=month(MONTH, label=TRUE) , col=Source, group=Source))  +
  geom_point() + geom_line()+  facet_wrap(~YEAR) + ggtitle("Area 19 (JDF)") +scale_x_discrete(guide = guide_axis(angle = 90)) +
  xlab("Month") + ylab("Proportion marked")+ theme_bw()


p19jdf

p20


MR_pre2019 + MR_post2019 +  plot_layout(guides = "collect")


PFMAMonth_biodata_plot2 + PFMAMonth_biodata_plot + plot_layout(guides = "collect")



PFMAMonth_biodata_plot

PFMAMonth_biodata_diff_recent_plot_1
PFMAMonth_biodata_diff_recent_plot_2
PFMAMonth_biodata_diff_recent_plot_3

PFMAMonth_biodata_potential_recent_plot_1
PFMAMonth_biodata_potential_recent_plot_2
PFMAMonth_biodata_potential_recent_plot_3



p11
p111
p12
p13
p14
p15
p16
p17
p18
p19gs
p19jdf
p20
p20e
p20w
p21
p121
p23a
p23b
p123
p24
p124
p25
p125
p26
p126
p27
p127
p28
p29










