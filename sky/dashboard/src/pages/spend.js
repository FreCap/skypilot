import React from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';

const EstimatedSpend = dynamic(
  () =>
    import('@/components/estimated-spend').then((mod) => mod.EstimatedSpend),
  { ssr: false }
);

export default function SpendPage() {
  return (
    <>
      <Head>
        <title>Estimated Compute Cost | SkyPilot Dashboard</title>
      </Head>
      <EstimatedSpend />
    </>
  );
}
