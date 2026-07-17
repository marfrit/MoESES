# MoESES — MoE Streaming Expert Server

**Serving DeepSeek-class MoE models on 2× NVIDIA DGX Spark by streaming routed experts on demand from a cheap SSD/RAM storage box over RoCE.**

> Status: **design stage / call for builders.** The topology is specced, the Sparks exist, the storage box does not (yet). If you build this before I do, I get my evaluation for free and you get a YouTube video. Everybody wins. Issues and results very welcome.

## The problem

DeepSeek V3/R1-class models are ~671B parameters. Two DGX Sparks give you 256 GB of combined LPDDR5X — not enough for the full weight set, even at int4 (~370 GB for one expert set). But MoE models only *activate* ~37B parameters per token, and expert selection is highly predictable (~72% router hit rate on a warm cache in prior work).

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
   QSFP112   QSFP112   (2× 1-port CX-6 Dx, 200G each)
```

- **bosch ↔ escher**: NCCL / RoCE clustering link for the resident/dense shards (NVIDIA-approved QSFP112 DAC; a generic FS 400G DAC has also been observed training at 200G).
- **bosch ↔ muse** and **escher ↔ muse**: expert stream, NVMe-oF over RDMA (RoCEv2), QSFP56 200G DACs. QSFP56 DACs plug directly into QSFP112 cages (same 4-lane form factor; the CX-7 negotiates down).
- **muse NIC — read this before you buy:** a ConnectX-6 Dx does **2× 100G *or* 1× 200G**, never 2× 200G — its PCIe 4.0 x16 bus caps the whole card at 200G aggregate. So the *dual-port* `MCX623106…-CDAT` gives each Spark only **100G**. To feed **200G to each Spark**, muse needs **two single-port 200G cards** (`MCX623105…-VDAT`), one per PCIe x16 slot, one DAC per Spark → 400G aggregate egress, which the 8-channel RAM cache can source on hits. (Bonding two 100G MACs into 200G is a *Spark-side* ConnectX-7 quirk; muse's native 200G ports skip that dance.)

### GB10 ConnectX-7 quirks you must know

- Each physical QSFP cage carries **2× 100G MACs on two separate PCIe Gen5 x4 links**. One MAC unbonded = ~100 Gbit/s. To reach 200G per cage you must bond the two MACs **across the two different x4 links** — bonding same-link MACs caps at ~92–95 Gbit/s.
- Many units ship firmware-pinned at ~13–16 Gbit/s despite negotiating 200G. Fix: ConnectX-7 firmware update **plus a full cold power-drain** (not just a reboot). Verify with Perftest before believing any number.
- The "second cage shows DOWN" symptom is a cabling artifact, not a hardware lock. Both cages are independently usable; RoCE to third-party endpoints works (validated by third parties with stock perftest — see ServeTheHome's GB10 networking deep-dive).

## Why 256 GB RAM on the storage box is the whole point

Both Sparks read the **same** read-only expert set. The storage box's Linux page cache therefore acts as a **shared warm-expert L2**: an expert pulled for bosch is already RAM-resident when escher asks. EPYC's 8 DDR4 channels (~150 GB/s) fan that cache out to both links. RAM size directly buys aggregate tok/s.

> **Mid-2026 caveat:** the DDR4 price spike (see parts list) has made this tier far pricier than at first draft. The *bandwidth* argument stands — RAM latency/throughput for the shared cache can't be matched by SSD. The *€/token* argument is now weak: at ~€8/GB for RDIMM vs ~€0.12/GB for NVMe, the RAM tier costs ~65× per GB of the SSD miss tier. Size the RAM tier to what you can afford and let the SSD stripe absorb more misses.

The SSDs cover cache misses.

## Parts list (storage box "muse")

Prices **researched 2026-07-17** against [Geizhals.de](https://geizhals.de) and eBay.de — not estimated. Headline finding: the **2025–26 DRAM + NAND supercycle** roughly **doubled** this build versus the first-draft floor. DDR4 RDIMM and 2 TB NVMe both spiked hard. Buy **used**; treat *new* retail as the ceiling.

| Part | SKU | ~€ (mid-2026) |
|---|---|---|
| Motherboard | ASRock Rack **ROMED8-2T** or Supermicro **H12SSL-i** (SP3, PCIe 4.0). *Avoid proprietary-WIO boards (e.g. H12SSW-NT) unless doing a rackmount build — they need risers + a WIO chassis.* | ROMED8-2T **~790 new** (Geizhals); H12SSL-i used **~450–650** |
| CPU | AMD **EPYC 7302P** (16c Rome; "P" = 1P-locked, cheaper, fine for single-socket muse) or **7313** (Milan) | 7302P **353–445 new** (Geizhals); **~130–200 used** (eBay.de). Verify **not vendor/PSB-locked** and **not an ES/QS** |
| RAM | **8× 32 GB DDR4-3200 ECC RDIMM** (Samsung M393A4K40DB3-CWE) | **~552/stick new DE** (362 EU) → ~4.4k new; **~2100 used** for the 8-kit (~262/stick — a *good* price today) |
| NIC | **2× ConnectX-6 Dx single-port 200GbE** (MCX623105A{S,N}-VDAT), one DAC per Spark. *A CX-6 Dx is 2×100G **or** 1×200G — the dual-port MCX623106-CDAT can't do 200G/port. Needs two x16 slots; ROMED8-2T has 7.* | **~400–700 ea used** (estimate) **× 2** |
| Cables | 2× QSFP56 200G DAC (FS Q56-PC0xx / Mellanox MCP1650-V001E30); keep a QSFP28 100G DAC as fallback | **~40–80 ea** |
| SSDs | 4× Gen4 NVMe 2 TB striped (~28 GB/s) — WD **SN850X** or Lexar **NM790** — on an ASUS Hyper M.2 x16 carrier (board must expose x4x4x4x4 bifurcation; H12SSL-i and ROMED8-2T do) | **~230–275 ea** (Geizhals; SN850X ~275, NM790 ~230 — was ~100) |
| Cooler / PSU / case | Noctua NH-U14S TR4-SP3, 650 W Gold, any case with airflow over the M.2 carrier | **~280** (estimate) |

**Realistic all-in (used where sane): ~€4.8–5.9 k** — up from the first draft's €1.7–2.8 k. RAM and NVMe drove most of it; the corrected NIC spec (two single-port 200G cards, not one dual-port) adds ~€0.4–0.9 k. New-retail all-in is far higher (RAM alone €2.9–4.4 k). The NVMe-oF target does essentially no CPU work — you are buying **PCIe lanes and memory channels**, not cores.

**Budget lever — 8× 16 GB, not 4× 32 GB.** You must populate all 8 channels for bandwidth, so you can't just buy half the sticks (that drops muse to 4-channel and halves fan-out). **8× 16 GB (128 GB)** keeps full 8-channel bandwidth at roughly half the RAM cost — the sane v1; expand later (DDR4 won't get cheaper).

Compute side (not included): 2× DGX Spark, which you presumably already regret buying.

*Prices are point-in-time snapshots (2026-07-17) from German retail (Geizhals) and used listings (eBay.de). NIC, DAC, and case rows are estimates pending firm quotes. Verify before buying — this market moves weekly.*

## Expected performance (back of envelope)

DeepSeek int4, ~37B active params/token, most of it routed experts:

| Link config | Bandwidth | Est. token ceiling* | muse NIC needed† |
|---|---|---|---|
| 100G per Spark (day 1) | ~12.5 GB/s | ~2–4 tok/s | 1× dual-port 2×100G (MCX623106-CDAT) |
| 200G per Spark | ~25 GB/s | ~5–8 tok/s | 2× single-port 200G (MCX623105-VDAT) |

\* Assuming ~72% of expert fetches are absorbed by the muse RAM tier + locally pinned hot experts, leaving ~3–5 GB of miss traffic per token. These numbers are hypotheses to be destroyed by measurement, not promises.

† A ConnectX-6 Dx caps at 200G aggregate (PCIe 4.0 x16), so one card cannot do 200G to *both* Sparks — hence two single-port cards for the 200G row. The "bonded" 200G is a Spark-side ConnectX-7 detail (2×100G MACs → 200G per cage); muse's 200G ports are native and need no bonding.

The big software lever: **router-logit prefetching** — request next-layer experts while the current layer computes, hiding link latency behind GPU work.

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
3. **Naive baseline:** mmap DeepSeek expert set over NVMe-oF, run unmodified llama.cpp, measure. Zero code.
4. **Bonding:** 2× 100G MACs → 200G per Spark, re-measure.
5. **Async prefetcher:** router-logit-driven expert prefetch in the llama.cpp fork. This is where the tok/s lives.

## Risk register

- RoCE to third-party target on *this specific* DGX-OS image: high confidence, unverified. Milestone 1 exists for exactly this. NVMe/TCP is the fallback.
- QSFP112 cage ↔ QSFP56 DAC negotiation: expected fine, test before bulk-buying cables.
- **Memory + NAND supercycle (materialized).** The "DDR4 rising" risk hit hard: as of 2026-07 this RDIMM SKU is ~€552/stick new (Geizhals) and 2 TB NVMe ~€230–275 — together roughly doubling the build. Used is now the only sane path, and prices aren't coming back down soon. Buy RAM first; consider the 8× 16 GB start.
- Used EPYC hazard: confirm the CPU is **not vendor/PSB-locked** to an OEM and is **not an engineering sample** before paying.
- NIC gotcha (see topology): budget for **two** single-port 200G cards if you want 200G per Spark — one dual-port card can't do it.
- The prefetcher is real systems work. The hardware is the easy half.

## License / contributing

Do whatever you want with this design. If you build it, open an issue with your numbers — especially tok/s at milestones 3–5 and whether the QSFP56 DAC trained cleanly. Benchmarks > opinions.

---

*Named after the guy who parted a large body of water so his people could cross. Here it's expert weights crossing a QSFP link, served by a host called `muse` to two Sparks called `bosch` and `escher`.*
