# MoESES — MoE Streaming Expert Server

**Serving DeepSeek-class MoE models on 2× NVIDIA DGX Spark by streaming routed experts on demand from a cheap SSD/RAM storage box over RoCE.**

> Status: design stage, call for builders. The topology is specced, the Sparks exist, the storage box does not (yet). If you build it before I do, I get my evaluation for free and you get a YouTube video. Issues and results very welcome.

## The problem

DeepSeek V3/R1-class models run to 671B total parameters, of which about 37B are active per token ([DeepSeek-V3 report](https://arxiv.org/abs/2412.19437)). Two DGX Sparks give you 256 GB of combined LPDDR5X. That is not enough for the full weight set even at 4-bit: naive int4 comes to ~335 GB, and a real DeepSeek-V3 Q4_K_M GGUF weighs ~404 GB. But an MoE model activates only ~37B params per token, and which experts it picks is fairly predictable (~72% hit rate on a warm cache in prior work; the performance section explains why that number carries the whole design).

The plan follows from that. Keep the dense and shared weights plus the hot experts resident on the Sparks, and page the cold experts over the network from a box whose only jobs are a large shared page cache and fast sequential reads.

There is prior art. The [Colibri](https://github.com/example) approach pages experts with `pread()` on Apple Silicon, CPU-only, and tops out around 1 tok/s. MoESES borrows the storage idea but feeds GPU inference instead, which moves the bottleneck off the pager and onto the link, and is what makes 200G networking worth the trouble.

## Topology: switchless triangle

Each DGX Spark has 2× QSFP112 ports (ConnectX-7). Three point-to-point DAC links, no switch:

```
        bosch (DGX Spark)
        /               \
   QSFP112            QSFP112
      /                   \
 escher (DGX) --------- muse (storage box)
   QSFP112   QSFP112   (2× 1-port 200G RoCE NIC)
```

- **bosch ↔ escher**: NCCL / RoCE clustering link for the resident/dense shards (NVIDIA-approved QSFP112 DAC; a generic FS 400G DAC has also been observed training at 200G).
- **bosch ↔ muse** and **escher ↔ muse**: expert stream, NVMe-oF over RDMA (RoCEv2), QSFP56 200G DACs. QSFP56 DACs plug directly into QSFP112 cages (same 4-lane form factor; the CX-7 negotiates down).
- **muse NIC, read this before you buy**: a ConnectX-6 Dx does either 2× 100G or 1× 200G, never 2× 200G, because its PCIe 4.0 x16 bus caps the whole card at 200G aggregate ([datasheet](https://www.nvidia.com/content/dam/en-zz/Solutions/networking/ethernet-adapters/connectX-6-dx-datasheet.pdf)). To feed 200G to each Spark you need two single-port 200G cards, one per PCIe x16 slot, one DAC per Spark, for 400G of aggregate egress, which the 8-channel RAM cache can source on hits. muse runs a software nvmet-rdma target, so it needs a 200G RoCE NIC but not the Dx-specific offloads, which opens up cheaper parts (see the NIC row). Bonding two 100G MACs into 200G is a Spark-side ConnectX-7 quirk; native 200G ports skip that step.

### GB10 ConnectX-7 quirks you must know

- Each physical QSFP cage carries **2× 100G MACs on two separate PCIe Gen5 x4 links**. One MAC unbonded is ~100 Gbit/s. To reach 200G per cage you have to bond the two MACs across the two different x4 links; bonding same-link MACs caps at ~92–95 Gbit/s.
- Many units ship firmware-pinned at ~13–16 Gbit/s despite negotiating 200G. The fix is a ConnectX-7 firmware update plus a full cold power-drain, not just a reboot. Verify with Perftest before believing any number.
- The "second cage shows DOWN" symptom is a cabling artifact, not a hardware lock. Both cages are independently usable, and RoCE to third-party endpoints works (validated by third parties with stock perftest; see ServeTheHome's GB10 networking deep-dive).

## Why 256 GB RAM on the storage box is the whole point

Both Sparks read the same read-only expert set, so the storage box's Linux page cache doubles as a shared warm-expert L2: an expert pulled for bosch is already RAM-resident when escher asks for it. EPYC's 8 DDR4 channels (~150 GB/s realistic, 205 GB/s theoretical) fan that cache out to both links. RAM size buys you SSD-miss avoidance.

> **Mid-2026 caveat:** the DDR4 price spike (see the parts list) has made this tier much pricier than it was at first draft. The bandwidth-and-latency argument still holds, since RAM for the shared cache is not something SSD can match. The €/token argument is now weak: at ~€8/GB for RDIMM against ~€0.12/GB for NVMe, the RAM tier costs about 65× per GB what the SSD miss tier does. Note too (see performance) that a muse-RAM hit still crosses the link, so the RAM tier saves SSD latency, not link bandwidth. Size it to what you can afford and let the SSD stripe absorb more misses.

## Parts list (storage box "muse")

Prices **researched 2026-07-17** against [Geizhals.de](https://geizhals.de), German resellers, and eBay.de, not estimated. The headline: the 2025–26 DRAM and NAND supercycle, plus 200G NIC pricing, roughly tripled this build versus the first-draft floor. Buy used where a used market exists (RAM, board, CPU, SSD); the NICs mostly don't have one in DE.

| Part | SKU | ~€ (mid-2026) |
|---|---|---|
| Motherboard | ASRock Rack **ROMED8-2T** or Supermicro **H12SSL-i** (SP3, PCIe 4.0). *Avoid proprietary-WIO boards (e.g. H12SSW-NT) unless doing a rackmount build — they need risers + a WIO chassis.* | ROMED8-2T **~790 new** (Geizhals); H12SSL-i used **~450–650** |
| CPU | AMD **EPYC 7302P** (16c Rome; "P" = 1P-locked, cheaper, fine for single-socket muse) or **7313** (Milan) | 7302P **353–445 new** (Geizhals); **~130–200 used** (eBay.de). Verify **not vendor/PSB-locked** and **not an ES/QS** |
| RAM | **8× 32 GB DDR4-3200 ECC RDIMM** (Samsung M393A4K40DB3-CWE) | **~552/stick new DE** (362 EU) → ~4.4k new; **~2100 used** for the 8-kit (~262/stick — a *good* price today) |
| NIC (200G/Spark) | **2× single-port 200GbE RoCE**, one DAC per Spark. muse needs 200G RoCE, **not** Dx offloads. Cheapest new in DE: ConnectX-6 VPI **MCX653105A-HDAT ~1283** (fs.com). Dx VDAT: **AC-VDAT ~1396** (servershop-bayern), **AN-VDAT ~1575** (Geizhals); AS-VDAT is EOL/import-only. Used -VDAT rarely listed in DE. | **~1283–1575 ea new → ~2.6–3.2k for two** |
| NIC (100G/Spark day-1 alt) | 1× dual-port **MCX623106AN-CDAT** (2×100G), or a used ConnectX-5 2×100G | ~866 new (Geizhals) / ~150–300 used |
| Cables | 2× QSFP56 200G DAC (FS Q56-PC0xx / Mellanox MCP1650-V001E30); keep a QSFP28 100G DAC as fallback | **~40–80 ea** |
| SSDs | 4× Gen4 NVMe 2 TB striped (~28 GB/s) — WD **SN850X** or Lexar **NM790** — on an ASUS Hyper M.2 x16 carrier (x4x4x4x4 bifurcation; H12SSL-i and ROMED8-2T do) | **~230–275 ea** (Geizhals; was ~100) |
| Cooler / PSU / case | Noctua NH-U14S TR4-SP3, 650 W Gold, airflow over the M.2 carrier | **~280** (estimate) |

Realistic all-in, used where a used market exists:

- 200G-per-Spark target: ~€6.8–7.6 k. The two 200G NICs (~€2.6–3.2 k new) now rival RAM as the biggest line, so 200G networking is about 40% of the build. That is the real price of the 5–8 tok/s tier.
- 100G-per-Spark day-1: ~€4.4–5.1 k. One dual-port 2×100G card (~€866 new, or a used CX-5 ~€200) gets the 2–4 tok/s baseline; upgrade the NICs to reach 200G later.

That is up from the first draft's €1.7–2.8 k. RAM, NVMe, and the 200G NICs each roughly doubled or worse. New-retail all-in is higher still (RAM alone is €2.9–4.4 k new). The NVMe-oF target does essentially no CPU work, so you are buying PCIe lanes and memory channels, not cores.

**Budget lever, 8× 16 GB rather than 4× 32 GB.** All 8 channels have to be populated for bandwidth, so you can't just buy half the sticks (that drops muse to 4-channel and halves fan-out). 8× 16 GB (128 GB) keeps full 8-channel bandwidth at roughly half the RAM cost, which is a sane v1; expand later.

Compute side (not included): 2× DGX Spark, which you presumably already regret buying, and the economics section below explains why.

*Prices are point-in-time snapshots (2026-07-17) from German retail (Geizhals, servershop-bayern, fs.com) and used listings (eBay.de). DAC and case rows remain estimates. Verify before buying; this market moves weekly.*

## Expected performance (back of envelope)

At int4, DeepSeek-V3 activates 37B params per token. About 20B of that is routed-expert weight (8 experts × 58 MoE layers × ~44M params/expert), which is the streamable part; the other ~17B (MLA attention, 3 dense layers, shared expert, embeddings) stays resident. So the streamable load is about 10 GB per token at int4 (20B × 0.5 B). The split is derived from the [published config](https://arxiv.org/abs/2412.19437).

Only the part of that ~10 GB not already resident crosses the link. The two Sparks hold 256 GB of LPDDR5X; a naive-int4 model is ~335 GB, of which ~327 GB is routed experts. After dense weights (~8 GB) and KV-cache, activation, and OS overhead (~40 GB), roughly 207 GB is left for experts, so about 55–63% of the routed-expert weight pins locally. With hot-expert access skew that gives an estimated 65–72% local hit rate, which works out to ~3–5 GB paged from muse per token.

| Link config | Link BW | Paged/token | Est. token ceiling | muse NIC |
|---|---|---|---|---|
| 100G per Spark (day 1) | ~12.5 GB/s | ~3–5 GB | ~2–4 tok/s | 1× dual-port 2×100G |
| 200G per Spark | ~25 GB/s | ~3–5 GB | ~5–8 tok/s | 2× single-port 200G |

The link is the bottleneck, and two independent ceilings confirm it:

- Spark memory-bound ceiling: 18.5 GB/token (37B × 0.5 B) ÷ 273 GB/s LPDDR5X ≈ 15 tok/s per Spark.
- Compute-bound ceiling: ~74 GFLOP/token (2×37e9) against GB10's 1 PFLOP FP4, so >1000 tok/s.

Both sit far above the link-bound 5–8, so the link binds first, which is the reason MoESES exists. Spark specs verified at 128 GB and 273 GB/s per unit, 1 PFLOP FP4 ([StorageReview](https://www.storagereview.com/review/nvidia-dgx-spark-review-the-ai-appliance-bringing-datacenter-capabilities-to-desktops), [NVIDIA](https://www.nvidia.com/en-us/products/workstations/dgx-spark/)).

**⚠️ The assumption everything rests on.** That ~72% is a local-residency hit rate: weights already on the Spark, zero link traffic. It is not a muse-RAM hit, because muse-RAM bytes still cross the link (the RAM tier saves SSD latency, not link bandwidth). The number is borrowed from prior CPU-cache work and is unverified for DeepSeek-V3 on this hardware, and the whole table swings on it: 55% local hit gives ~3 tok/s, 85% gives ~11. Measure it before trusting any tok/s figure here.

Performance lever: a lighter dynamic-4-bit build (~340–370 GB) instead of Q4_K_M (~404 GB) raises local residency, which raises the hit rate, which raises tok/s. Quant choice is a throughput knob, not just a disk-space one.

The big software lever is still router-logit prefetching: request next-layer experts while the current layer computes, hiding link latency behind GPU work.

## Does this even pencil out?

No. If the goal is cheaper DeepSeek tokens, stop here and use the API; on price per token this build loses to it by more than tenfold, and it is fairer to say so before anyone spends money.

The running cost sinks it on its own. At about 6 tok/s the machine pulls roughly 550 W (140 W per Spark, ~270 W for muse). A million output tokens take about 46 hours to produce, call it 25 kWh, or ~€9 of German electricity at €0.35/kWh ([BDEW 2026](https://www.cleanenergywire.org/factsheets/what-german-households-pay-electricity)). The DeepSeek V3 API charges about €0.37 for the same million output tokens ($0.40; V4 Flash is $0.28, [DeepSeek pricing](https://api-docs.deepseek.com/quick_start/pricing/)). So the electricity by itself runs about 24 times the API price, before any hardware.

Capital never comes back. Run it flat out around the clock and it produces roughly 189M tokens a year, worth about €70 at V3 rates:

| | Cost | Payback vs API |
|---|---|---|
| muse box (Sparks treated as sunk) | ~€7 k | ~100 years |
| muse + 2× DGX Spark (~€4.5 k ea) | ~€16 k | ~230 years |
| Electricity, 24/7 | ~€1.7 k/yr | operating cost alone ≈ 24× the token value |

By the time any of that pays off the hardware is obsolete. Batching concurrent requests spreads each expert fetch across several sequences and helps the per-token cost, but not by a factor of 24, so the API still wins.

There are good reasons to build it anyway, and none of them are financial. Data stays in the building, which for some work is the entire point. It runs offline and air-gapped, with no metering, rate limits, or terms of service. It doesn't depend on a provider that can change under you: DeepSeek retired `R1` into `V4 Flash` in mid-2026 with about a month's notice, and a model you have pinned locally can't be swapped out. And it is a research project, where the async expert pager is the part worth doing. The free evaluation and the YouTube video were always the actual reward.

If you want cheap DeepSeek tokens, use the API at ~€0.37/M and skip the soldering iron. If you want the capability in your own hands, read on.

## Software plan

**Storage (muse):** stock Linux plus `nvmet` / `nvmet-rdma`, exporting the mdraid0 stripe as one namespace. `nvmet-tcp` is the guaranteed fallback if RoCEv2 PFC/ECN tuning fights DGX-OS.

**Sparks:** `nvme connect -t rdma` (or `-t tcp`), then mount/mmap the remote namespace as the expert store.

**Inference engine:** a fork of **llama.cpp**, not vLLM. The reasoning:

- llama.cpp already loads weights via `mmap`, so you can mount the NVMe-oF namespace and have experts page over the link on day one with zero code changes. Slow and blind, but a real baseline.
- MoE routing lives in the ggml graph, and the point where router logits are known before the expert FFNs execute is where the async prefetcher hooks in.
- vLLM assumes GPU-resident weights at load, and PagedAttention pages the KV-cache, not the weights. Retrofitting an SSD tier means fighting the model loader, the scheduler, and PyTorch memory management at once.
- Study first: **ktransformers** (GPU plus CPU/RAM expert offload with router-aware placement, the closest existing design) and **DeepSpeed ZeRO-Inference** NVMe offload (batch-oriented, but the offload code exists).

The missing piece to build: ggml has no notion of "this tensor is not resident yet, stall or prefetch" mid-graph. That async expert pager, plus its CUDA-side integration, is the actual project.

## Milestones

1. **Protocol validation (0 €):** `nvme connect -t rdma` from bosch to a throwaway `nvmet-rdma` target on any Linux box with any RDMA NIC (an old ConnectX-3/4 will do). Proves DGX-OS talks NVMe-oF/RoCE to third-party hardware before money moves.
2. **muse build + link bring-up:** firmware-update and cold-drain both Sparks, perftest each link, verify the QSFP56 DAC trains in the QSFP112 cage.
3. **Naive baseline:** mmap the DeepSeek expert set over NVMe-oF, run unmodified llama.cpp, measure, and measure the real local-residency hit rate, the number the whole design rests on.
4. **Bonding:** 2× 100G MACs to 200G per Spark, re-measure.
5. **Async prefetcher:** router-logit-driven expert prefetch in the llama.cpp fork. This is where the tok/s lives.

## Risk register

- RoCE to a third-party target on this specific DGX-OS image: high confidence, unverified. Milestone 1 exists for exactly this. NVMe/TCP is the fallback.
- The 72% local-residency hit rate is the design's single biggest unknown (see performance). It's borrowed from other work and unmeasured here, and tok/s scales almost linearly with it. Milestone 3 has to measure it before any hardware past the baseline is justified.
- NIC cost. Single-port 200G cards are ~€1.3–1.6 k each new in DE and you need two (~€2.6–3.2 k), about 40% of the build, and used -VDAT rarely surfaces. muse needs only a 200G RoCE NIC (not Dx offloads), so a ConnectX-6 VPI (MCX653105A-HDAT) or a used CX-5/CX-6 is a legitimate cheaper substitute; verify RoCE plus nvmet-rdma on it first.
- Economics (see above). This never beats the DeepSeek API on cost, since electricity alone is ~24× the API token price. Build only if sovereignty, offline operation, or the research is worth it to you.
- Memory and NAND supercycle, now real. This RDIMM SKU is ~€552/stick new and 2 TB NVMe ~€230–275 as of 2026-07. Used is the only sane path; buy RAM first, and consider the 8× 16 GB start.
- Used EPYC hazard: confirm the CPU is not vendor/PSB-locked and not an engineering sample before paying.
- QSFP112 cage to QSFP56 DAC negotiation: expected to be fine, but test before bulk-buying cables.
- The prefetcher is real systems work. The hardware is the easy half.

## License / contributing

Do whatever you want with this design. If you build it, open an issue with your numbers, especially the measured local-residency hit rate and tok/s at milestones 3–5, and whether the QSFP56 DAC trained cleanly. Benchmarks beat opinions.

---

*Named after the guy who parted a large body of water so his people could cross. Here it's expert weights crossing a QSFP link, served by a host called `muse` to two Sparks called `bosch` and `escher`.*
