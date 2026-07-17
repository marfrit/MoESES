# MoESES — MoE Streaming Expert Server

**Serving DeepSeek-class MoE models on 2× NVIDIA DGX Spark by streaming routed experts on demand from a cheap SSD/RAM storage box over RoCE.**

> Status: **design stage / call for builders.** The topology is specced, the Sparks exist, the storage box does not (yet). If you build this before I do, I get my evaluation for free and you get a YouTube video. Everybody wins. Issues and results very welcome.

## The problem

DeepSeek V3/R1-class models are **671B total parameters, ~37B activated per token** (verified: [DeepSeek-V3 report](https://arxiv.org/abs/2412.19437)). Two DGX Sparks give you 256 GB of combined LPDDR5X — not enough for the full weight set, even at 4-bit (**~335 GB naive int4; a real DeepSeek-V3 Q4_K_M GGUF weighs ~404 GB**). But MoE models only *activate* ~37B params per token, and expert selection is highly predictable (~72% hit rate on a warm cache in prior work — see the flashing-light caveat under performance).

So: keep the dense/shared weights and hot experts resident on the Sparks, and **page cold experts over the network** from a box whose only jobs are (a) a big shared page cache and (b) fast sequential reads.

Prior art: the [Colibri](https://github.com/example) approach does expert paging via `pread()` on Apple Silicon, CPU-only, ceiling ~1 tok/s. MoESES is the same storage idea, but feeding **GPU inference** — which moves the bottleneck from the pager to the link, and makes 200G networking actually worth having.

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
- **muse NIC — read this before you buy:** a ConnectX-6 Dx does **2× 100G *or* 1× 200G**, never 2× 200G — its PCIe 4.0 x16 bus caps the whole card at 200G aggregate ([datasheet](https://www.nvidia.com/content/dam/en-zz/Solutions/networking/ethernet-adapters/connectX-6-dx-datasheet.pdf)). To feed **200G to each Spark** you need **two single-port 200G cards**, one per PCIe x16 slot, one DAC per Spark → 400G aggregate egress, which the 8-channel RAM cache can source on hits. muse runs a *software* nvmet-rdma target, so it needs only a **200G RoCE** NIC — **not** the Dx-specific offloads — which opens up cheaper parts (see the NIC row). (Bonding two 100G MACs into 200G is a *Spark-side* ConnectX-7 quirk; native 200G ports skip that dance.)

### GB10 ConnectX-7 quirks you must know

- Each physical QSFP cage carries **2× 100G MACs on two separate PCIe Gen5 x4 links**. One MAC unbonded = ~100 Gbit/s. To reach 200G per cage you must bond the two MACs **across the two different x4 links** — bonding same-link MACs caps at ~92–95 Gbit/s.
- Many units ship firmware-pinned at ~13–16 Gbit/s despite negotiating 200G. Fix: ConnectX-7 firmware update **plus a full cold power-drain** (not just a reboot). Verify with Perftest before believing any number.
- The "second cage shows DOWN" symptom is a cabling artifact, not a hardware lock. Both cages are independently usable; RoCE to third-party endpoints works (validated by third parties with stock perftest — see ServeTheHome's GB10 networking deep-dive).

## Why 256 GB RAM on the storage box is the whole point

Both Sparks read the **same** read-only expert set. The storage box's Linux page cache therefore acts as a **shared warm-expert L2**: an expert pulled for bosch is already RAM-resident when escher asks. EPYC's 8 DDR4 channels (~150 GB/s realistic; 205 GB/s theoretical) fan that cache out to both links. RAM size buys SSD-miss avoidance.

> **Mid-2026 caveat:** the DDR4 price spike (see parts list) has made this tier far pricier than at first draft. The *bandwidth/latency* argument stands — RAM for the shared cache can't be matched by SSD. The *€/token* argument is now weak: at ~€8/GB for RDIMM vs ~€0.12/GB for NVMe, the RAM tier costs ~65× per GB of the SSD miss tier. Also note (see performance) that **muse-RAM hits still cross the link** — the RAM tier saves SSD latency, not link bandwidth. Size it to what you can afford and let the SSD stripe absorb more misses.

## Parts list (storage box "muse")

Prices **researched 2026-07-17** against [Geizhals.de](https://geizhals.de), German resellers, and eBay.de — not estimated. Headline finding: the **2025–26 DRAM + NAND supercycle** plus **200G NIC pricing** roughly **tripled** this build versus the first-draft floor. Buy **used** where a used market exists (RAM, board, CPU, SSD); NICs mostly don't have one in DE.

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

**Realistic all-in (used where a used market exists):**
- **200G-per-Spark target: ~€6.8–7.6 k.** The two 200G NICs (~€2.6–3.2 k new) now rival RAM as the biggest line — **200G networking is ~40% of the build.** That is the real price of the 5–8 tok/s tier.
- **100G-per-Spark day-1: ~€4.4–5.1 k.** One dual-port 2×100G card (~€866 new, or a used CX-5 ~€200) gets the 2–4 tok/s baseline; upgrade the NICs to reach 200G later.

Up from the first draft's €1.7–2.8 k — RAM, NVMe, *and* the 200G NICs each roughly doubled or worse. New-retail all-in is higher still (RAM alone €2.9–4.4 k new). The NVMe-oF target does essentially no CPU work — you are buying **PCIe lanes and memory channels**, not cores.

**Budget lever — 8× 16 GB, not 4× 32 GB.** All 8 channels must be populated for bandwidth, so you can't just buy half the sticks (that drops muse to 4-channel and halves fan-out). **8× 16 GB (128 GB)** keeps full 8-channel bandwidth at roughly half the RAM cost — a sane v1; expand later.

Compute side (not included): 2× DGX Spark, which you presumably already regret buying — and the economics section below explains why.

*Prices are point-in-time snapshots (2026-07-17) from German retail (Geizhals, servershop-bayern, fs.com) and used listings (eBay.de). DAC and case rows remain estimates. Verify before buying — this market moves weekly.*

## Expected performance (back of envelope)

DeepSeek-V3 at int4, 37B active params/token — of which **~20B are routed-expert weight** (8 experts × 58 MoE layers × ~44M params/expert), the streamable part; the other ~17B (MLA attention, 3 dense layers, shared expert, embeddings) stays resident. So the **per-token streamable load ≈ 10 GB at int4** (20B × 0.5 B). *(Split derived from the [published config](https://arxiv.org/abs/2412.19437).)*

**Only the not-already-resident fraction of that ~10 GB crosses the link.** The two Sparks hold 256 GB LPDDR5X; a naive-int4 model is ~335 GB (~327 GB of it routed experts). After dense weights (~8 GB) and KV-cache / activation / OS overhead (~40 GB), roughly **~207 GB — about 55–63% of the routed-expert weight — pins locally.** With hot-expert access skew, that gives an estimated **~65–72% local hit rate → ~3–5 GB paged from muse per token.**

| Link config | Link BW | Paged/token | Est. token ceiling | muse NIC |
|---|---|---|---|---|
| 100G per Spark (day 1) | ~12.5 GB/s | ~3–5 GB | ~2–4 tok/s | 1× dual-port 2×100G |
| 200G per Spark | ~25 GB/s | ~3–5 GB | ~5–8 tok/s | 2× single-port 200G |

**The link is the bottleneck — confirmed two independent ways:**
- **Spark memory-bound ceiling:** 18.5 GB/token (37B × 0.5 B) ÷ 273 GB/s LPDDR5X ≈ **~15 tok/s** per Spark.
- **Compute-bound ceiling:** ~74 GFLOP/token (2×37e9) vs GB10's **1 PFLOP FP4** → **>1000 tok/s.**

Both are far above the link-bound 5–8, so the link binds first — which is the entire reason MoESES exists. *(Spark specs verified: 128 GB/273 GB/s per unit, 1 PFLOP FP4 — [StorageReview](https://www.storagereview.com/review/nvidia-dgx-spark-review-the-ai-appliance-bringing-datacenter-capabilities-to-desktops), [NVIDIA](https://www.nvidia.com/en-us/products/workstations/dgx-spark/).)*

**⚠️ The load-bearing assumption.** The ~72% is a **local-residency** hit rate (weights on the Spark, zero link traffic) — **not** a muse-RAM hit, because muse-RAM bytes still cross the link (the RAM tier saves *SSD latency*, not *link bandwidth*). It is imported from prior CPU-cache work, **unverified for DeepSeek-V3 on this hardware**, and the whole table swings on it: at 55% local hit ≈ 3 tok/s, at 85% ≈ 11. **Measure this before trusting any tok/s number here.**

**Performance lever:** a lighter dynamic-4-bit build (~340–370 GB) instead of Q4_K_M (~404 GB) raises local residency → raises the hit rate → raises tok/s. Quant choice is a throughput knob, not just a disk-space one.

The big software lever remains **router-logit prefetching** — request next-layer experts while the current layer computes, hiding link latency behind GPU work.

## Does this even pencil out? (no)

Build MoESES for sovereignty, offline operation, or research — **not** to save money. On pure €/token it loses to the DeepSeek API by more than an order of magnitude, and that's worth stating plainly before anyone orders parts.

**The killer number is electricity, before any hardware.** At ~6 tok/s (mid of the 200G range) the rig draws ~550 W (2× Spark @ ~140 W + muse ~270 W). One million output tokens takes ~46 h to generate → ~25 kWh → **~€9 in German electricity** (@ ~€0.35/kWh, [BDEW 2026](https://www.cleanenergywire.org/factsheets/what-german-households-pay-electricity)). The **DeepSeek V3 API charges ~€0.37 per 1M output tokens** ($0.40; V4 Flash is $0.28 — [DeepSeek pricing](https://api-docs.deepseek.com/quick_start/pricing/)). So **generating locally costs ~24× the API price in electricity alone** — before a cent of hardware.

**Capital never amortizes.** Flat-out 24/7 the rig makes ~189M tokens/year, worth **~€70/year** at V3 API rates:

| | Cost | Payback vs API |
|---|---|---|
| muse box (Sparks treated as sunk) | ~€7 k | ~100 years |
| muse + 2× DGX Spark (~€4.5 k ea) | ~€16 k | ~230 years |
| Electricity, 24/7 | ~€1.7 k/yr | operating cost alone ≈ 24× the token value |

The hardware is obsolete long before payback. Batch/throughput serving amortizes expert fetches across concurrent sequences and improves €/token — but nowhere near 24×; the API still wins on cost.

**So why build it?** The reasons are real, just not financial:
- **Data sovereignty** — prompts and outputs never leave your premises. For some work that's the whole ballgame.
- **Offline / air-gapped** operation — no internet dependency, no metering, no rate limits, no ToS.
- **No provider risk** — DeepSeek folded `R1` into `V4 Flash` mid-2026 on ~a month's notice; a local model you've pinned can't be retired under you.
- **It's a research project** — the async expert pager is the interesting part. "Call for builders / free evaluation / YouTube video" was always the actual point.

If you want cheap DeepSeek tokens, use the API — ~€0.37/M, no soldering iron. If you want to *own* the capability, read on.

## Software plan

**Storage (muse):** stock Linux + `nvmet` / `nvmet-rdma` exporting the mdraid0 stripe as one namespace. `nvmet-tcp` is the guaranteed fallback if RoCEv2 PFC/ECN tuning fights DGX-OS.

**Sparks:** `nvme connect -t rdma` (or `-t tcp`), then mount/mmap the remote namespace as the expert store.

**Inference engine:** fork of **llama.cpp**, not vLLM. Rationale:

- llama.cpp already loads weights via `mmap` — mount the NVMe-oF namespace and experts page over the link on day one with zero code changes (slow and blind, but a real baseline).
- MoE routing lives in the ggml graph; the point where router logits are known before expert FFNs execute is where the async prefetcher hooks in.
- vLLM assumes GPU-resident weights at load; PagedAttention pages KV-cache, not weights. Retrofitting an SSD tier means fighting the model loader, scheduler, and PyTorch memory management simultaneously.
- Study first: **ktransformers** (GPU + CPU/RAM expert offload, router-aware placement — closest existing design) and **DeepSpeed ZeRO-Inference** NVMe offload (batch-oriented, but the offload code exists).

The missing piece to build: ggml has no concept of "this tensor is not resident yet, stall/prefetch" mid-graph. That async expert pager + CUDA-side integration is the actual project.

## Milestones

1. **Protocol validation (0 €):** `nvme connect -t rdma` from bosch to a throwaway `nvmet-rdma` target on any Linux box with any RDMA NIC (an old ConnectX-3/4 suffices). Proves DGX-OS talks NVMe-oF/RoCE to third-party hardware before money moves.
2. **muse build + link bring-up:** firmware-update + cold-drain both Sparks, perftest each link, verify QSFP56 DAC trains in the QSFP112 cage.
3. **Naive baseline:** mmap DeepSeek expert set over NVMe-oF, run unmodified llama.cpp, measure — *and measure the real local-residency hit rate, the number the whole design rests on.*
4. **Bonding:** 2× 100G MACs → 200G per Spark, re-measure.
5. **Async prefetcher:** router-logit-driven expert prefetch in the llama.cpp fork. This is where the tok/s lives.

## Risk register

- RoCE to third-party target on *this specific* DGX-OS image: high confidence, unverified. Milestone 1 exists for exactly this. NVMe/TCP is the fallback.
- **The 72% local-residency hit rate is the design's single biggest unknown** (see performance). It's borrowed from other work and unmeasured here; tok/s scales almost linearly with it. Milestone 3 must measure it before any hardware past the baseline is justified.
- **NIC cost shock.** Single-port 200G cards are ~€1.3–1.6 k each new in DE and you need two (~€2.6–3.2 k) — ~40% of the build; used -VDAT rarely surfaces. muse needs only a 200G RoCE NIC (not Dx offloads), so a ConnectX-6 VPI (MCX653105A-HDAT) or a used CX-5/CX-6 is a legitimate cheaper substitute — verify RoCE + nvmet-rdma on it first.
- **Economics (see above).** This never beats the DeepSeek API on cost — electricity alone is ~24× the API token price. Build only if sovereignty/offline/research is worth it to you.
- **Memory + NAND supercycle (materialized).** This RDIMM SKU is ~€552/stick new and 2 TB NVMe ~€230–275 as of 2026-07. Used is the only sane path; buy RAM first, consider the 8× 16 GB start.
- Used EPYC hazard: confirm the CPU is **not vendor/PSB-locked** and **not an engineering sample** before paying.
- QSFP112 cage ↔ QSFP56 DAC negotiation: expected fine, test before bulk-buying cables.
- The prefetcher is real systems work. The hardware is the easy half.

## License / contributing

Do whatever you want with this design. If you build it, open an issue with your numbers — especially the measured local-residency hit rate and tok/s at milestones 3–5, and whether the QSFP56 DAC trained cleanly. Benchmarks > opinions.

---

*Named after the guy who parted a large body of water so his people could cross. Here it's expert weights crossing a QSFP link, served by a host called `muse` to two Sparks called `bosch` and `escher`.*
