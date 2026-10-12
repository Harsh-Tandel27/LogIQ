import type { NextConfig } from "next";

const apiUrl = process.env.LOGIQ_API_URL ?? "http://localhost:8000";

const nextConfig: NextConfig = {
  // The browser calls /api/* on this origin; Next.js proxies it to FastAPI.
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${apiUrl}/api/:path*` }];
  },
};

export default nextConfig;
