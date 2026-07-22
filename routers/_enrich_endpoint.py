# Enrich endpoint — to be appended to assemble.py
# Receives tool call results from the mesh controller,
# runs the intelligence pipeline, returns enriched context.

class EnrichRequest(BaseModel):
    tool_name: str
    arguments: dict = {}
    result: Optional[str] = None
    duration_ms: int = 0
    success: bool = True
    session_id: Optional[str] = None


@router.post("/enrich")
async def enrich_tool_result(req: EnrichRequest):
    """Post-execution intelligence hook.
    
    Called by the mesh controller AFTER every tool execution.
    Helix evaluates the result and decides what pipeline to run:
    - File writes → full pipeline (version → scan → index → KG → observe)
    - Commands with output → observer + auto-detect scan
    - Read-only → observer only
    
    Returns enriched text (atom suggestions) to append to the tool result,
    plus pipeline status so the controller knows what happened.
    """
    pipeline_ran = []
    atoms_created = 0
    enriched_text = ""
    version_id = ""
    
    try:
        # 1. Classify the tool call
        is_file_write = any(kw in req.tool_name for kw in [
            'file_write', 'file_patch', 'helix_file_write'
        ])
        is_command = any(kw in req.tool_name for kw in [
            'ssh_execute', 'host.exec', 'helix_command', 'mesh_exec'
        ])
        has_content = bool(req.result and len(str(req.result)) > 20)
        
        # 2. For file writes: trigger the workbench pipeline
        #    (version → scan → index → KG → observe)
        if is_file_write and has_content:
            path = req.arguments.get('path', '')
            content = req.arguments.get('content', '')
            if path and content:
                try:
                    from services.workbench import WorkbenchService
                    wb = WorkbenchService()
                    # Don't re-write the file (already written by the tool)
                    # Just run the intelligence pipeline
                    from services.events.file_events import dispatch_file_written
                    result = await dispatch_file_written(
                        path=path,
                        content=content,
                        session_id=req.session_id or 'mesh-controller',
                        steps={
                            'git': True,
                            'scan': True,
                            'kb': True,
                            'kg': True,
                            'forge': True,
                            'shard': False,
                            'observer': True,
                        }
                    )
                    pipeline_ran = [k for k, v in result.items() if v and isinstance(v, dict) and v.get('status') == 'ok']
                    if not pipeline_ran:
                        pipeline_ran = list(result.keys())
                except Exception as e:
                    log.warning(f"enrich pipeline for {req.tool_name}: {e}")
                    pipeline_ran.append(f"error:{e}")
        
        # 3. Observer logging for ALL tool calls
        try:
            from services.observer import get_observer
            obs = get_observer()
            obs.log_action(
                tool=req.tool_name,
                arguments=req.arguments,
                result_summary=str(req.result)[:500] if req.result else '',
                duration_ms=req.duration_ms,
                success=req.success,
                session_id=req.session_id or 'mesh-controller',
            )
            pipeline_ran.append('observer')
        except Exception as e:
            log.debug(f"observer log failed: {e}")
        
        # 4. Suggest relevant atoms for context enrichment
        if has_content:
            try:
                from services.assembler import get_assembler
                asm = get_assembler()
                # Build task description from tool + args
                task = f"{req.tool_name}"
                if 'path' in req.arguments:
                    task += f" {req.arguments['path']}"
                if 'command' in req.arguments:
                    task += f" {str(req.arguments['command'])[:100]}"
                
                # Keyword-based archetype matching
                keywords = task.lower().split()
                archetype_map = {
                    'docker': ['docker_compose', 'dockerfile'],
                    'deploy': ['docker_compose', 'deployment'],
                    'router': ['fastapi_router', 'api_endpoint'],
                    'api': ['fastapi_router', 'api_endpoint'],
                    'mesh': ['mesh_controller', 'infrastructure'],
                    'helix': ['helix_cortex', 'intelligence'],
                }
                archetypes = []
                for kw in keywords:
                    for key, vals in archetype_map.items():
                        if key in kw:
                            archetypes.extend(vals)
                archetypes = list(dict.fromkeys(archetypes))[:3]
                
                if archetypes:
                    result = asm.assemble_by_archetype(archetypes, mode='documentation')
                    atoms_used = result.get('atoms_used', [])
                    atoms_created = len(atoms_used)
                    if atoms_used:
                        atom_names = [a.get('name', a.get('atom_id', '?')) for a in atoms_used[:3]]
                        enriched_text = f"[Helix] Relevant atoms: {', '.join(atom_names)}"
            except Exception as e:
                log.debug(f"atom suggestion failed: {e}")
        
        # 5. Version step: if we enriched the output, the enriched version
        #    is itself new content that should be captured
        if enriched_text and is_file_write:
            try:
                from services.observer import get_observer
                obs = get_observer()
                obs.log_action(
                    tool='helix_enrich_version',
                    arguments={'original_tool': req.tool_name, 'enriched_length': len(enriched_text)},
                    result_summary=enriched_text[:200],
                    duration_ms=0,
                    success=True,
                    session_id=req.session_id or 'mesh-controller',
                )
                pipeline_ran.append('version_step')
            except Exception:
                pass
        
        return JSONResponse({
            'enriched_text': enriched_text,
            'pipeline_ran': pipeline_ran,
            'atoms_created': atoms_created,
            'version_id': version_id,
            'tool_name': req.tool_name,
            'success': True,
        })
    except Exception as e:
        log.error(f"enrich endpoint: {e}")
        return JSONResponse({
            'enriched_text': '',
            'pipeline_ran': pipeline_ran,
            'atoms_created': 0,
            'version_id': '',
            'error': str(e),
            'success': False,
        }, status_code=200)  # always 200 — never block the pipeline
