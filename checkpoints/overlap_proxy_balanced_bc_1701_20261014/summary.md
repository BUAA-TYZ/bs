# Episode-Equal BC Fit

L = (1/E) sum_e (1/N_e) sum_valid_tile_labels_in_e CE

For B uniformly shuffled rows among R: L_hat = R/(E*B) sum_batch_valid_labels CE/N_episode

This is a fit diagnostic on cached training demonstrations. No validation/test performance is claimed.

Rows: 54310; tile labels: 216924; optimizer updates: 3210.
Final episode-equal loss: 0.030317; overall fit agreement: 0.997732.

| Action | Labels | Recall |
|---|---:|---:|
| 0 | 214640 | 0.998900 |
| 1 | 2244 | 0.893048 |
| 2 | 38 | 0.631579 |
| 10 | 2 | 0.000000 |
