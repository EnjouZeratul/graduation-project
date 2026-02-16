<template>
  <div ref="chartRef" class="chart"></div>
</template>

<script lang="ts" setup>
import * as echarts from "echarts";
import { onBeforeUnmount, onMounted, ref, watch } from "vue";
import type { RegionDetail } from "@/services/api";

const props = defineProps<{
  regions: RegionDetail[];
  selectedRegion: any | null;
}>();

const emit = defineEmits<{
  (e: "region-click", regionCode: string): void;
}>();

const chartRef = ref<HTMLDivElement | null>(null);
let chart: echarts.ECharts | null = null;

const riskColorMap: Record<string, string> = {
  green: "#5fbf88",
  yellow: "#d9ba56",
  orange: "#dc9154",
  red: "#cf5f5a"
};
const riskValueMap: Record<string, number> = {
  green: 0,
  yellow: 1,
  orange: 2,
  red: 3
};

function areaStyleByLevel(level?: string) {
  switch (level) {
    case "green":
      return {
        areaColor: {
          type: "linear",
          x: 0.2,
          y: 0,
          x2: 0.9,
          y2: 1,
          colorStops: [
            { offset: 0, color: "#86d4a8" },
            { offset: 1, color: "#4ba879" }
          ]
        },
        borderColor: "#6fd3a3"
      };
    case "yellow":
      return {
        areaColor: {
          type: "linear",
          x: 0.2,
          y: 0,
          x2: 0.9,
          y2: 1,
          colorStops: [
            { offset: 0, color: "#efd788" },
            { offset: 1, color: "#c8a549" }
          ]
        },
        borderColor: "#e8cc72"
      };
    case "orange":
      return {
        areaColor: {
          type: "linear",
          x: 0.2,
          y: 0,
          x2: 0.9,
          y2: 1,
          colorStops: [
            { offset: 0, color: "#f2b286" },
            { offset: 1, color: "#cc7c40" }
          ]
        },
        borderColor: "#e6a271"
      };
    case "red":
      return {
        areaColor: {
          type: "linear",
          x: 0.2,
          y: 0,
          x2: 0.9,
          y2: 1,
          colorStops: [
            { offset: 0, color: "#e99692" },
            { offset: 1, color: "#b94e4a" }
          ]
        },
        borderColor: "#df8f8b"
      };
    default:
      return {
        areaColor: "#4b6279",
        borderColor: "#95aec5"
      };
  }
}

function riskLabel(level?: string) {
  switch (level) {
    case "green":
      return "低风险";
    case "yellow":
      return "较低风险";
    case "orange":
      return "较高风险";
    case "red":
      return "高风险";
    default:
      return "未知";
  }
}

function confidenceLabel(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "未知";
  const pct = Math.round(Number(value) * 1000) / 10;
  return pct % 1 === 0 ? `${pct.toFixed(0)}%` : `${pct.toFixed(1)}%`;
}

function buildRegionMeta() {
  const riskByCode: Record<string, { level: string; confidence?: number | null }> = {};
  props.regions.forEach((r) => {
    riskByCode[String(r.code)] = {
      level: r.latest_warning?.level || r.risk_level || "green",
      confidence: r.latest_warning?.confidence
    };
  });
  return riskByCode;
}

async function initChart() {
  if (!chartRef.value) return;
  chart = echarts.init(chartRef.value);

  const geoJsonUrl = "https://geo.datav.aliyun.com/areas_v3/bound/100000_full_city.json";

  let geoJson: any | null = null;
  try {
    const resp = await fetch(geoJsonUrl);
    if (resp.ok) geoJson = await resp.json();
  } catch (e) {
    console.error("加载 GeoJSON 失败:", e);
  }

  if (geoJson) {
    try {
      echarts.registerMap("china", geoJson as any);
    } catch {
      // ignore if already registered
    }
  }

  const baseOption: echarts.EChartsOption = {
    backgroundColor: "transparent",
    visualMap: {
      type: "piecewise",
      orient: "horizontal",
      left: "center",
      bottom: 6,
      itemWidth: 14,
      itemHeight: 9,
      itemGap: 14,
      textStyle: {
        color: "#c5d5e2",
        fontSize: 11
      },
      pieces: [
        { value: 0, label: "低风险", color: riskColorMap.green },
        { value: 1, label: "较低风险", color: riskColorMap.yellow },
        { value: 2, label: "较高风险", color: riskColorMap.orange },
        { value: 3, label: "高风险", color: riskColorMap.red }
      ]
    },
    tooltip: {
      trigger: "item",
      formatter: (params: any) => {
        const data = params.data || {};
        return `${data.name || ""}<br/>风险等级：${riskLabel(data.riskLevel)}<br/>置信度：${data.confidenceLabel || "未知"}`;
      }
    }
  };

  const riskByCode = buildRegionMeta();
  let series: any[] = [];

  if (geoJson?.features) {
    series = [
      {
        name: "风险等级",
        type: "map",
        map: "china",
        roam: true,
        label: { show: false },
        emphasis: { label: { show: false } },
        data: geoJson.features.map((feat: any) => {
          const p = feat.properties || {};
          const adcode = String(p.adcode || p.adcode14 || "");
          const name = p.name || p.fullname || p.adname || "";
          const meta = riskByCode[adcode] || { level: "green", confidence: null };
          return {
            name,
            value: riskValueMap[meta.level] ?? 0,
            regionCode: adcode,
            riskLevel: meta.level,
            confidenceLabel: confidenceLabel(meta.confidence),
            itemStyle: areaStyleByLevel(meta.level)
          };
        }),
        itemStyle: {
          borderColor: "rgba(208, 224, 238, 0.78)",
          borderWidth: 1.15
        },
        emphasis: {
          itemStyle: {
            borderColor: "rgba(241, 248, 255, 0.98)",
            borderWidth: 1.8,
            shadowBlur: 10,
            shadowColor: "rgba(40, 54, 68, 0.35)"
          }
        }
      }
    ];
  } else {
    series = [
      {
        name: "风险等级",
        type: "map",
        map: "china",
        roam: true,
        itemStyle: { borderColor: "#1f2937" },
        data: props.regions.map((r) => ({
          name: r.name,
          value: riskValueMap[r.latest_warning?.level || r.risk_level || "green"] ?? 0,
          regionCode: r.code,
          riskLevel: r.latest_warning?.level || r.risk_level,
          confidenceLabel: confidenceLabel(r.latest_warning?.confidence),
          itemStyle: areaStyleByLevel(r.latest_warning?.level || r.risk_level)
        }))
      }
    ];
  }

  chart.setOption({ ...baseOption, series });

  chart.on("click", (params: any) => {
    if (params?.data?.regionCode) emit("region-click", params.data.regionCode);
  });
}

watch(
  () => props.regions,
  () => {
    if (!chart) {
      initChart();
      return;
    }

    try {
      const opt = chart.getOption();
      const mapSeriesIndex = (opt.series || []).findIndex((s: any) => s.type === "map");
      if (mapSeriesIndex === -1) return;

      const riskByCode = buildRegionMeta();
      const currentSeries = (opt.series || [])[mapSeriesIndex] || {};
      const newData = (currentSeries.data || []).map((d: any) => {
        const code = String(d.regionCode || "");
        const meta = riskByCode[code] || { level: d.riskLevel || "green", confidence: null };
        return {
          ...d,
          value: riskValueMap[meta.level] ?? 0,
          riskLevel: meta.level,
          confidenceLabel: confidenceLabel(meta.confidence),
          itemStyle: areaStyleByLevel(meta.level)
        };
      });
      chart.setOption({ series: [{ data: newData }] });
    } catch {
      // ignore
    }
  },
  { deep: true }
);

onMounted(() => {
  initChart();
  window.addEventListener("resize", () => {
    chart?.resize();
  });
});

onBeforeUnmount(() => {
  chart?.dispose();
});
</script>

<style scoped>
.chart {
  width: 100%;
  height: 100%;
  min-height: 520px;
  border-radius: 12px;
  background:
    radial-gradient(560px 280px at 12% 10%, rgba(194, 221, 240, 0.08), transparent 64%),
    linear-gradient(180deg, rgba(16, 26, 40, 0.16), rgba(14, 23, 34, 0.08));
}
</style>
