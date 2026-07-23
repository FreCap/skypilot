import React from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';

const Images = dynamic(
  () => import('@/components/images').then((mod) => mod.Images),
  { ssr: false }
);

export default function ImagesPage() {
  return (
    <>
      <Head>
        <title>Images | SkyPilot Dashboard</title>
      </Head>
      <Images />
    </>
  );
}
