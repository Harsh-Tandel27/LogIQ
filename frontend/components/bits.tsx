import Link from "next/link";
import type { Severity } from "@/lib/api";

const SEVERITY_STYLE: Record<Severity, string> = {
  High: "border-crit/30 bg-crit-wash text-crit",
  Medium: "border-warn/30 bg-warn-wash text-warn",
  Low: "border-rule bg-sunken text-ink-2",
};

export function SeverityBadge({ severity }: { severity: Severity }) {
  // Icon shape differs per level so severity never depends on colour alone.
  const icon =
    severity === "High" ? (
      <path d="M5 1 9.5 9h-9L5 1Z" fill="currentColor" />
    ) : severity === "Medium" ? (
      <rect x="1.5" y="1.5" width="7" height="7" rx="1" fill="currentColor" transform="rotate(45 5 5)" />
    ) : (
      <circle cx="5" cy="5" r="3.2" stroke="currentColor" strokeWidth="1.4" fill="none" />
    );
  return (
    <span className={`inline-flex items-center gap-1 rounded border px-1.5 py-px text-xs font-medium ${SEVERITY_STYLE[severity]}`}>
      <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden>{icon}</svg>
      {severity}
    </span>
  );
}

export function StatusBadge({ status }: { status: "open" | "resolved" }) {
  return status === "open" ? (
    <span className="inline-flex items-center gap-1.5 text-xs font-medium text-crit">
      <span aria-hidden className="live-dot h-1.5 w-1.5 rounded-full bg-crit" />
      Open
    </span>
  ) : (
    <span className="text-xs font-medium text-good">Resolved</span>
  );
}

export function PageHeader({
  crumbs,
  title,
  meta,
  actions,
}: {
  crumbs?: { href?: string; label: string }[];
  title: React.ReactNode;
  meta?: React.ReactNode;
  actions?: React.ReactNode;
}) {
  return (
    <header className="border-b border-rule bg-surface px-4 py-3 sm:px-6">
      {crumbs && (
        <nav aria-label="Breadcrumb" className="mb-1 text-xs text-ink-3">
          {crumbs.map((c, i) => (
            <span key={c.label}>
              {i > 0 && <span className="mx-1.5">/</span>}
              {c.href ? <Link href={c.href} className="hover:text-ink">{c.label}</Link> : c.label}
            </span>
          ))}
        </nav>
      )}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-lg font-semibold">{title}</h1>
          {meta}
        </div>
        {actions && <div className="flex items-center gap-2">{actions}</div>}
      </div>
    </header>
  );
}

export function Panel({
  title,
  meta,
  children,
  flush,
  className = "",
  id,
}: {
  title?: React.ReactNode;
  meta?: React.ReactNode;
  children: React.ReactNode;
  /** No inner padding, for tables that run edge to edge. */
  flush?: boolean;
  className?: string;
  id?: string;
}) {
  return (
    <section id={id} className={`overflow-hidden rounded-md border border-rule bg-surface ${className}`}>
      {title && (
        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-rule px-3 py-2">
          <h2 className="text-[13px] font-semibold">{title}</h2>
          {meta && <div className="text-xs text-ink-3">{meta}</div>}
        </div>
      )}
      <div className={flush ? "" : "p-3"}>{children}</div>
    </section>
  );
}

export function ErrorNote({ error }: { error: string | null }) {
  if (!error) return null;
  return (
    <p role="status" className="rounded border border-crit/30 bg-crit-wash px-3 py-2 text-[13px] text-crit">
      API unreachable ({error}). Start it with <code className="font-mono">make api</code>.
    </p>
  );
}

export function Empty({ children }: { children: React.ReactNode }) {
  return <p className="px-3 py-8 text-center text-[13px] text-ink-3">{children}</p>;
}

export const buttonPrimary =
  "inline-flex h-8 items-center justify-center gap-1.5 rounded bg-brand px-3 py-1.5 text-[13px] font-medium text-white hover:bg-[#0b5c5c] disabled:opacity-50";
export const buttonDanger =
  "inline-flex h-8 items-center justify-center gap-1.5 rounded bg-crit px-3 py-1.5 text-[13px] font-medium text-white hover:bg-[#a82217] disabled:opacity-50";
export const buttonSecondary =
  "inline-flex h-8 items-center justify-center gap-1.5 rounded border border-rule-strong bg-surface px-3 py-1.5 text-[13px] font-medium text-ink hover:bg-sunken disabled:opacity-40";
export const field = "w-full rounded border border-rule-strong bg-surface px-2 py-1.5 text-[13px] focus:border-brand focus:outline-none";
