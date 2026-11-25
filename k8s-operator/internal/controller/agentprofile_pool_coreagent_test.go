package controller

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// A core-agent profile's worker pod renders its persona before the worker
// starts: an init container, as a user the worker isn't, writes it into an
// emptyDir the worker mounts read-only where the harness reads it, and the
// worker is told the persona is required.
func TestACoreAgentProfilePodRendersItsPersonaFirst(t *testing.T) {
	for _, imageVolumes := range []bool{true, false} {
		t.Run(map[bool]string{true: "image volumes", false: "staged images"}[imageVolumes], func(t *testing.T) {
			checkCoreAgentPersonaRender(t, imageVolumes)
		})
	}
}

func checkCoreAgentPersonaRender(t *testing.T, imageVolumes bool) {
	agent := a2aTestAgent()
	profile := testAgentProfile(agent.Namespace, "auditor")
	profile.Spec.Harness.Type = coreAgentHarnessType
	spec := buildProfileWorkerPodSpec(agent, &profile, "read", "agentprofile-auditor-read", profileWorkerPool, imageVolumes)

	var render *corev1.Container
	for i := range spec.InitContainers {
		if spec.InitContainers[i].Name == coreAgentPersonaRenderName {
			render = &spec.InitContainers[i]
		}
	}
	if render == nil {
		t.Fatalf("no %s init container in %v", coreAgentPersonaRenderName, spec.InitContainers)
	}
	if spec.InitContainers[len(spec.InitContainers)-1].Name != coreAgentPersonaRenderName {
		t.Error("the render isn't the last init container, after the persona is staged")
	}
	if render.Image != profile.Spec.Harness.Image {
		t.Errorf("render image %q, want the harness image %q", render.Image, profile.Spec.Harness.Image)
	}
	wantCmd := []string{"/usr/local/bin/worker-adapter", "render-persona", "--harness", "core-agent", "--in", "/persona", "--out", coreAgentPersonaRenderPath + "/persona"}
	if !reflect.DeepEqual(render.Command, wantCmd) {
		t.Errorf("render command %v, want %v", render.Command, wantCmd)
	}
	sc := render.SecurityContext
	if sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != coreAgentPersonaRenderUID || *sc.RunAsUser == profileWorkerUID ||
		sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot || sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation ||
		sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem || sc.Capabilities == nil || !slices.Equal(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("render security context %+v", sc)
	}
	// Only the broker's address: no credential, token file or bus setting.
	var names []string
	for _, e := range render.Env {
		names = append(names, e.Name)
		if e.ValueFrom != nil {
			t.Errorf("render env %s comes from a reference", e.Name)
		}
	}
	if !slices.Equal(names, []string{"CREDENTIAL_PROXY_URL"}) {
		t.Errorf("render env %v, want only CREDENTIAL_PROXY_URL", names)
	}
	for _, m := range render.VolumeMounts {
		if m.Name != "persona" && m.Name != profileWorkerAgentPersonaVolume {
			t.Errorf("the render mounts %s", m.Name)
		}
	}
	// The render reads the persona as the worker does: the same mount, the
	// payload subPath under image volumes included.
	var renderPersona, workerPersona corev1.VolumeMount
	for _, m := range render.VolumeMounts {
		if m.Name == "persona" {
			renderPersona = m
		}
	}
	for _, m := range spec.Containers[0].VolumeMounts {
		if m.Name == "persona" {
			workerPersona = m
		}
	}
	if !reflect.DeepEqual(renderPersona, workerPersona) || !renderPersona.ReadOnly || renderPersona.MountPath != "/persona" {
		t.Errorf("the render's persona mount %+v, want the worker's %+v", renderPersona, workerPersona)
	}
	if imageVolumes && renderPersona.SubPath != "files" {
		t.Errorf("under image volumes the render's persona subPath is %q, want files", renderPersona.SubPath)
	}
	// Requests and limits, as a quota or Autopilot needs.
	for _, r := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		if _, ok := render.Resources.Requests[r]; !ok {
			t.Errorf("the render requests no %s", r)
		}
		if _, ok := render.Resources.Limits[r]; !ok {
			t.Errorf("the render has no %s limit", r)
		}
	}

	worker := spec.Containers[0]
	var mount *corev1.VolumeMount
	for i := range worker.VolumeMounts {
		if worker.VolumeMounts[i].Name == profileWorkerAgentPersonaVolume {
			mount = &worker.VolumeMounts[i]
		}
	}
	if mount == nil || mount.MountPath != "/var/lib/agent-persona" || mount.SubPath != "persona" || !mount.ReadOnly {
		t.Errorf("the worker's persona mount %+v, want read-only persona at /var/lib/agent-persona", mount)
	}
	env := envOf(worker)
	if env[coreAgentPersonaRequiredEnv] != "true" {
		t.Errorf("%s = %q, want true", coreAgentPersonaRequiredEnv, env[coreAgentPersonaRequiredEnv])
	}
	// The read-only persona is never emptied between tasks.
	if strings.Contains(env[profileWorkerResetPathsEnv], "/var/lib/agent-persona") {
		t.Errorf("the reset paths %q name the persona", env[profileWorkerResetPathsEnv])
	}
}

// A pod for a type the operator doesn't render as core-agent gets no render,
// no persona volume and no required flag.
func TestOtherProfilePodsGetNoPersonaRender(t *testing.T) {
	agent := a2aTestAgent()
	for _, h := range []string{"claude-code", "antigravity"} {
		profile := testAgentProfile(agent.Namespace, "auditor")
		profile.Spec.Harness.Type = h
		spec := buildProfileWorkerPodSpec(agent, &profile, "read", "agentprofile-auditor-read", profileWorkerPool, true)
		for _, c := range spec.InitContainers {
			if c.Name == coreAgentPersonaRenderName {
				t.Errorf("type %q: a persona render", h)
			}
		}
		for _, v := range spec.Volumes {
			if v.Name == profileWorkerAgentPersonaVolume {
				t.Errorf("type %q: the persona volume", h)
			}
		}
		if _, ok := envOf(spec.Containers[0])[coreAgentPersonaRequiredEnv]; ok {
			t.Errorf("type %q: %s set", h, coreAgentPersonaRequiredEnv)
		}
	}
}

// A profile with no type runs the default harness, core-agent, so its pod
// renders exactly what one naming core-agent does: the same persona render,
// volumes, mounts and required flag. Only A2A_HARNESS and the waiver beside
// it differ, rendered when the type is named; the worker reads an unset
// A2A_HARNESS as core-agent.
func TestAProfileWithNoTypeRendersAsCoreAgent(t *testing.T) {
	agent := a2aTestAgent()
	build := func(h string) corev1.PodSpec {
		p := testAgentProfile(agent.Namespace, "auditor")
		p.Spec.Harness.Type = h
		return buildProfileWorkerPodSpec(agent, &p, "read", "agentprofile-auditor-read", profileWorkerPool, true)
	}
	unset, named := build(""), build(coreAgentHarnessType)
	if !reflect.DeepEqual(unset.InitContainers, named.InitContainers) {
		t.Errorf("init containers differ:\nunset %+v\nnamed %+v", unset.InitContainers, named.InitContainers)
	}
	if !reflect.DeepEqual(unset.Volumes, named.Volumes) {
		t.Errorf("volumes differ:\nunset %+v\nnamed %+v", unset.Volumes, named.Volumes)
	}
	if !reflect.DeepEqual(unset.Containers[0].VolumeMounts, named.Containers[0].VolumeMounts) {
		t.Errorf("worker mounts differ:\nunset %+v\nnamed %+v", unset.Containers[0].VolumeMounts, named.Containers[0].VolumeMounts)
	}
	ue, ne := envOf(unset.Containers[0]), envOf(named.Containers[0])
	if ue[coreAgentPersonaRequiredEnv] != "true" || ne[coreAgentPersonaRequiredEnv] != "true" {
		t.Errorf("%s: unset %q, named %q; want true on both", coreAgentPersonaRequiredEnv, ue[coreAgentPersonaRequiredEnv], ne[coreAgentPersonaRequiredEnv])
	}
	for _, k := range []string{"A2A_HARNESS", "A2A_HARNESS_UNISOLATED"} {
		delete(ue, k)
		delete(ne, k)
	}
	if !reflect.DeepEqual(ue, ne) {
		t.Errorf("worker env differs beyond A2A_HARNESS:\nunset %v\nnamed %v", ue, ne)
	}
	if _, set := envOf(unset.Containers[0])["A2A_HARNESS"]; set {
		t.Error("an unset type rendered A2A_HARNESS; the worker's default stands for it")
	}
}
