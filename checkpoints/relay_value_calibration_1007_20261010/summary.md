# Critic Calibration

fixed stochastic BC actor; equal-case 50% decision/50% uniform-time fitting; same rows and update budget

unseen by critic fitting, sampled only from training distribution; not final test

| Stage | Fit active MSE | Probe active MSE | Probe normalized MSE | Probe explained variance |
|---|---:|---:|---:|---:|
| initial | 4.916126 | 5.538989 | 23.4175 | -0.0240 |
| sequential | 1.986278 | 1.844824 | 6.2932 | 0.5603 |
| pooled | 0.051368 | 0.709737 | 3.1641 | 0.2373 |

The actor is unchanged in every stage. Critic accuracy does not establish a scheduling improvement.
