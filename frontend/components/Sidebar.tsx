"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { usePoll } from "@/lib/api";

type Component = { name: string; ok: boolean; detail?: string | null };

const NAV = [
  {
    href: "/dashboard",
    label: "Overview",
    match: (p: string) => p.startsWith("/dashboard") || p.startsWith("/incident"),
    icon: (
      <path d="M2 13h3V7H2v6Zm4.5 0h3V3h-3v10Zm4.5 0h3V9h-3v4Z" fill="currentColor" />
    ),
  },
  {
    href: "/simulate",
    label: "Simulator",
    match: (p: string) => p.startsWith("/simulate"),
    icon: (
      <path d="M2 8h2.5l1.5-4 3 8 1.5-4H14" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinejoin="round" />
    ),
  },
];

function Logo() {
  // Many short lines collapsing into one: raw logs becoming a single alert.
  return (
    <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden>
      <rect width="22" height="22" rx="5" fill="var(--brand)" />
      <path d="M5 6h5M5 9h7M5 12h4M5 15h6" stroke="#fff" strokeOpacity=".55" strokeWidth="1.4" strokeLinecap="round" />
      <path d="M13 6.5 16.5 11 13 15.5" stroke="#fff" strokeWidth="1.6" fill="none" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export function Sidebar() {
  const pathname = usePathname();
  const { data: components } = usePoll<Component[]>("/api/components", 10000);

  return (
    <aside className="flex shrink-0 flex-row items-center gap-4 border-b border-rule bg-surface px-4 py-2 md:sticky md:top-0 md:h-screen md:w-56 md:flex-col md:items-stretch md:gap-0 md:border-r md:border-b-0 md:px-3 md:py-4">
      <Link href="/dashboard" className="flex items-center gap-2 md:px-2">
        <Logo />
        <span className="text-[15px] font-semibold tracking-tight">LogIQ</span>
        <span className="rounded bg-sunken px-1.5 py-px font-mono text-[11px] text-ink-3">local</span>
      </Link>

      <nav className="flex gap-1 md:mt-6 md:flex-col">
        {NAV.map((item) => {
          const active = item.match(pathname);
          return (
            <Link
              key={item.href}
              href={item.href}
              aria-current={active ? "page" : undefined}
              className={`flex items-center gap-2.5 rounded px-2 py-1.5 ${active ? "bg-brand-wash font-medium text-brand" : "text-ink-2 hover:bg-sunken hover:text-ink"}`}
            >
              <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden>{item.icon}</svg>
              {item.label}
            </Link>
          );
        })}
      </nav>

      <div className="mt-auto hidden md:block">
        <p className="px-2 pb-1.5 text-xs font-medium text-ink-3">Pipeline</p>
        <ul className="space-y-0.5">
          {(components ?? []).map((c) => (
            <li key={c.name} className="flex items-center justify-between gap-2 rounded px-2 py-1 text-[13px]" title={c.detail ?? undefined}>
              <span className="flex items-center gap-2">
                <span aria-hidden className={`h-1.5 w-1.5 rounded-full ${c.ok ? "bg-good" : c.name === "Slack" ? "bg-rule-strong" : "bg-crit"}`} />
                {c.name}
              </span>
              <span className={`text-xs ${c.ok ? "text-ink-3" : c.name === "Slack" ? "text-ink-3" : "text-crit"}`}>
                {c.ok ? "up" : c.name === "Slack" ? "off" : "down"}
              </span>
            </li>
          ))}
          {!components && <li className="px-2 text-xs text-ink-3">Checking…</li>}
        </ul>
      </div>
    </aside>
  );
}
