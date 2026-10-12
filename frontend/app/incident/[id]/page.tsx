import { IncidentView } from "@/components/IncidentView";

export default async function IncidentPage(props: PageProps<"/incident/[id]">) {
  const { id } = await props.params;
  return <IncidentView id={id} />;
}
