"""从冻结图像和窗口清单绘制真实分片几何；不生成合成遥感影像。"""

import argparse
import hashlib
import json
from pathlib import Path
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import BoundaryNorm
from matplotlib.patches import Rectangle
import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sim.workload import ImageTiling, window_starts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--distribution", default="examples/overlap_proxy/distribution.json")
    parser.add_argument("--profile", default="p0019")
    parser.add_argument("--image", default="data/workload_assets/dota_mirror/P0019.png")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    spec = json.loads(Path(args.distribution).read_text())
    profile = next(p for p in spec["workload"]["profiles"] if p["id"] == args.profile)
    geometry = ImageTiling.from_dict(profile["image_tiling"])
    picture = Image.open(args.image).convert("RGB")
    if picture.size != (geometry.width_px, geometry.height_px):
        raise ValueError("Image shape differs from the frozen profile")
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=False)
    xs = window_starts(geometry.width_px, geometry.window_px, geometry.stride_px)
    ys = window_starts(geometry.height_px, geometry.window_px, geometry.stride_px)
    x_count = np.zeros(geometry.width_px, dtype=np.int16)
    y_count = np.zeros(geometry.height_px, dtype=np.int16)
    for x in xs:
        x_count[x:x + geometry.window_px] += 1
    for y in ys:
        y_count[y:y + geometry.window_px] += 1
    coverage = y_count[:, None] * x_count[None, :]
    manifest = geometry.manifest()
    if int(coverage.sum()) != manifest["processed_pixels"] or coverage.min() < 1:
        raise AssertionError("Coverage differs from the simulator workload")
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 5.4), constrained_layout=True)
    extent = (0, geometry.width_px, geometry.height_px, 0)
    picture.thumbnail((1600, 1600))
    axes[0].imshow(picture, extent=extent)
    for tile in geometry.tiles():
        for x0, y0, x1, y1 in tile.windows_xyxy:
            axes[0].add_patch(Rectangle((x0, y0), x1 - x0, y1 - y0,
                                       fill=False, edgecolor="white", linewidth=0.45, alpha=0.75))
    for x0, y0, x1, y1 in geometry.tiles()[0].windows_xyxy:
        axes[0].add_patch(Rectangle((x0, y0), x1 - x0, y1 - y0,
                                   fill=False, edgecolor="#fa493d", linewidth=1.8))
    axes[0].set_title("Original image and windows\nRed: four windows in the first scheduling packet", fontsize=11)
    maximum = int(coverage.max())
    norm = BoundaryNorm(np.arange(0.5, maximum + 1.5), maximum)
    cmap = plt.get_cmap("YlGnBu", maximum)
    mesh = axes[1].imshow(coverage[::4, ::4], extent=extent, interpolation="nearest", cmap=cmap, norm=norm)
    colorbar = fig.colorbar(mesh, ax=axes[1], shrink=0.77, ticks=np.arange(1, maximum + 1))
    colorbar.set_label("Number of window services per source pixel", fontsize=9)
    axes[1].set_title("Pixel coverage multiplicity\nEdge-shift increases overlap at the last windows", fontsize=11)
    for axis in axes:
        axis.set_xlabel("Source image x (pixels)")
        axis.set_ylabel("Source image y (pixels)")
    fig.suptitle(f"{args.profile.upper()}: {geometry.width_px} x {geometry.height_px}, "
                 f"{manifest['num_windows']} windows, {manifest['num_tiles']} packets\n"
                 f"1024 window / 824 stride; raw {manifest['image_size_mb']:.3f} MB; "
                 f"window payload {manifest['packet_input_mb']:.3f} MB "
                 f"({manifest['coverage_multiplier']:.3f}x)", fontsize=12)
    fig.savefig(out / "geometry.png", dpi=180)
    fig.savefig(out / "geometry.pdf", dpi=180)
    plt.close(fig)
    metadata = {"args": vars(args), "image_sha256": hashlib.sha256(Path(args.image).read_bytes()).hexdigest(),
                "distribution_sha256": hashlib.sha256(Path(args.distribution).read_bytes()).hexdigest(),
                "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "workload": manifest, "coverage_sum_pixels": int(coverage.sum()),
                "coverage_min": int(coverage.min()), "coverage_max": maximum,
                "source": f"DOTA val {args.profile.upper()} via isaaccorley/dota fixed mirror; see workload calibration protocol",
                "display": "Original preview resized for display only; workload uses original pixels. "
                           "Coverage map sampled every four pixels only for display."}
    (out / "geometry.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps({"output": str(out), "coverage_range": [int(coverage.min()), maximum],
                      "coverage_multiplier": manifest["coverage_multiplier"]}))


if __name__ == "__main__":
    main()
