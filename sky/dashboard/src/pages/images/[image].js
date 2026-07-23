import React from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';

const ImageDetail = dynamic(
  () => import('@/components/image-detail').then((mod) => mod.ImageDetail),
  { ssr: false }
);

export default function ImageDetailPage() {
  return (
    <>
      <Head>
        <title>Image detail | SkyPilot Dashboard</title>
      </Head>
      <ImageDetail />
    </>
  );
}
