import os
from functools import reduce

from aiohttp import ClientSession

from illallangi.alfa.functions import cheap_hash, common, merge, recursive_get
from illallangi.alfa.jinja import AlfaJinja
from illallangi.k8sapi import API as K8S_API

from loguru import logger

from more_itertools import first

import yaml

from yarl import URL


class Renderer:
    def __init__(self, api, dump, name, session=None, jinja=None):
        self.api = (
            K8S_API(URL(api) if not isinstance(api, URL) else api)
            if not isinstance(api, K8S_API)
            else api
        )
        self.dump = dump
        self.name = name
        self.jinja = AlfaJinja(name) if jinja is None else jinja
        self.session = ClientSession() if session is None else session
        if not isinstance(self.session, ClientSession):
            raise TypeError(
                "Expected ClientSession; got %s" % type(self.session).__name__
            )

    async def render(self):
        logger.info(f"Rendering AlfaTemplate {self.name} in {await self.scope} scope")
        return await self.renders

    @property
    async def items(self):
        if "_items" not in self.__dict__ or self._items is None:
            logger.info(f'Getting {"s, ".join(await self.kinds)}s')
            self._items = {k: await self.get_items(k) for k in await self.kinds}
            for k in self._items:
                if self.dump:
                    with open(
                        os.path.join(
                            self.dump,
                            f"alfatemplate-{self.name}-{k.lower()}s.yaml",
                        ),
                        "w",
                    ) as outfile:
                        outfile.write(yaml.dump_all(self._items[k]))
                logger.info(f" - Got {len(self._items[k])} {k}(s)")
        return self._items

    @property
    async def objects(self):
        if "_objects" not in self.__dict__ or self._objects is None:
            logger.info("Getting Objects")
            self._objects = [
                {
                    "kind": (await self.child).kind,
                    "apiVersion": (await self.child).api_group.group_version,
                    "metadata": {
                        "labels": {
                            (await self.labels_name): recursive_get(
                                item,
                                f"metadata#labels#{await self.labels_name}",
                                sep="#",
                            )
                            or (await self.parent_kind).lower(),
                            (await self.labels_instance): recursive_get(
                                item,
                                f"metadata#labels#{await self.labels_instance}",
                                sep="#",
                            )
                            or recursive_get(item, "metadata.name", default=""),
                            (await self.labels_domain_name): recursive_get(
                                item,
                                f"metadata#labels#{await self.labels_domain_name}",
                                sep="#",
                            )
                            or recursive_get(item, "spec.domainName", default=""),
                            (await self.labels_component): recursive_get(
                                item,
                                f"metadata#labels#{await self.labels_component}",
                                sep="#",
                            )
                            or await self.component
                            or "",
                            (await self.labels_managed_by): self.name,
                        },
                        "namespace": recursive_get(item, "metadata.namespace"),
                        "ownerReferences": [
                            {
                                "apiVersion": recursive_get(i, "apiVersion"),
                                "blockOwnerDeletion": True,
                                "controller": False,
                                "kind": recursive_get(i, "kind"),
                                "name": recursive_get(i, "metadata.name"),
                                "uid": recursive_get(i, "metadata.uid"),
                            }
                            for i in (await self.items)[await self.parent_kind]
                            if (await self.owner_references)
                            and recursive_get(i, "metadata.uid")
                            == recursive_get(item, "metadata.uid")
                        ],
                    },
                    "selector": {
                        (await self.labels_name): recursive_get(
                            item, f"metadata#labels#{await self.labels_name}", sep="#"
                        )
                        or (await self.parent_kind).lower(),
                        (await self.labels_instance): recursive_get(
                            item,
                            f"metadata#labels#{await self.labels_instance}",
                            sep="#",
                        )
                        or recursive_get(item, "metadata.name", default=""),
                        (await self.labels_domain_name): recursive_get(
                            item,
                            f"metadata#labels#{await self.labels_domain_name}",
                            sep="#",
                        )
                        or recursive_get(item, "spec.domainName", default=""),
                        (await self.labels_component): recursive_get(
                            item,
                            f"metadata#labels#{await self.labels_component}",
                            sep="#",
                        )
                        or await self.component
                        or "",
                    },
                    "_name": "-".join(
                        [
                            i
                            for i in [
                                (await self.parent_kind).lower(),
                                recursive_get(item, "metadata.name"),
                                cheap_hash(recursive_get(item, "spec.domainName")),
                                await self.component,
                            ]
                            if i
                        ]
                    ),
                    "spec": recursive_get(item, "spec"),
                    "subsets": recursive_get(item, "subsets"),
                }
                for item in (await self.items)[await self.parent_kind]
            ]
            if self.dump:
                with open(
                    os.path.join(self.dump, f"alfatemplate-{self.name}-objects.yaml"),
                    "w",
                ) as outfile:
                    outfile.write(yaml.dump_all(self._objects))
            logger.info(f" - Got {len(self._objects)} Objects")
        return self._objects

    @property
    async def domains(self):
        if "_domains" not in self.__dict__ or self._domains is None:
            logger.info("Getting Domains")
            self._domains = [
                reduce(
                    merge,
                    [
                        reduce(
                            common,
                            [
                                o
                                for o in (await self.objects)
                                if recursive_get(o, "spec.domainName") == d
                            ],
                        ),
                        {
                            "metadata": {
                                "labels": {(await self.labels_instance): ""},
                                "ownerReferences": [
                                    {
                                        "apiVersion": recursive_get(i, "apiVersion"),
                                        "blockOwnerDeletion": True,
                                        "controller": False,
                                        "kind": recursive_get(i, "kind"),
                                        "name": recursive_get(i, "metadata.name"),
                                        "uid": recursive_get(i, "metadata.uid"),
                                    }
                                    for i in (await self.items)[await self.parent_kind]
                                    if (await self.owner_references)
                                    and recursive_get(i, "spec.domainName") == d
                                ],
                            },
                            "selector": {(await self.labels_instance): ""},
                            "_name": "-".join(
                                [
                                    i
                                    for i in [
                                        (await self.parent_kind).lower(),
                                        cheap_hash(d),
                                        await self.component,
                                    ]
                                    if i
                                ]
                            ),
                        },
                        {
                            "objects": [
                                o
                                for o in (await self.objects)
                                if recursive_get(o, "spec.domainName") == d
                            ]
                        },
                    ],
                )
                for d in {
                    recursive_get(item, "spec.domainName")
                    for item in (await self.items)[await self.parent_kind]
                }
            ]
            if self.dump:
                with open(
                    os.path.join(self.dump, f"alfatemplate-{self.name}-domains.yaml"),
                    "w",
                ) as outfile:
                    outfile.write(yaml.dump_all(self._domains))
            logger.info(f" - Got {len(self._domains)} Domains")
        return self._domains

    @property
    async def namespaces(self):
        if "_namespaces" not in self.__dict__ or self._namespaces is None:
            logger.info("Getting Namespaces")
            self._namespaces = [
                reduce(
                    merge,
                    [
                        reduce(
                            common,
                            [
                                o
                                for o in (await self.objects)
                                if recursive_get(o, "metadata.namespace") == n
                            ],
                        ),
                        {
                            "metadata": {
                                "labels": {(await self.labels_domain_name): ""},
                                "ownerReferences": [
                                    {
                                        "apiVersion": recursive_get(i, "apiVersion"),
                                        "blockOwnerDeletion": True,
                                        "controller": False,
                                        "kind": recursive_get(i, "kind"),
                                        "name": recursive_get(i, "metadata.name"),
                                        "uid": recursive_get(i, "metadata.uid"),
                                    }
                                    for i in (await self.items)[await self.parent_kind]
                                    if (await self.owner_references)
                                    and recursive_get(i, "metadata.namespace") == n
                                ],
                            },
                            "selector": {(await self.labels_domain_name): ""},
                            "_name": "-".join(
                                [
                                    i
                                    for i in [
                                        (await self.parent_kind).lower(),
                                        await self.component,
                                    ]
                                    if i
                                ]
                            ),
                        },
                        {
                            "domains": [
                                d
                                for d in (await self.domains)
                                if recursive_get(d, "metadata.namespace") == n
                            ],
                            "objects": [
                                o
                                for o in (await self.objects)
                                if recursive_get(o, "metadata.namespace") == n
                            ],
                        },
                    ],
                )
                for n in {
                    recursive_get(item, "metadata.namespace")
                    for item in (await self.items)[await self.parent_kind]
                }
            ]
            if self.dump:
                with open(
                    os.path.join(
                        self.dump,
                        f"alfatemplate-{self.name}-namespaces.yaml",
                    ),
                    "w",
                ) as outfile:
                    outfile.write(yaml.dump_all(self._namespaces))
            logger.info(f" - Got {len(self._namespaces)} Namespaces")
        return self._namespaces

    @property
    async def clusters(self):
        if "_clusters" not in self.__dict__ or self._clusters is None:
            logger.info("Getting Clusters")
            self._clusters = [
                reduce(
                    merge,
                    [
                        reduce(common, (await self.objects)),
                        {"metadata": {"namespace": None}},
                        {
                            "namespaces": (await self.namespaces),
                            "domains": (await self.domains),
                            "objects": (await self.objects),
                        },
                    ],
                )
            ]
            if self.dump:
                with open(
                    os.path.join(
                        self.dump,
                        f"alfatemplate-{self.name}-clusters.yaml",
                    ),
                    "w",
                ) as outfile:
                    outfile.write(yaml.dump_all(self._clusters))
            logger.info(f" - Got {len(self._clusters)} Clusters")
        return self._clusters

    @property
    async def renders(self):
        if "_renders" not in self.__dict__ or self._renders is None:
            logger.info("Getting Renders")
            self._renders = [
                merge(
                    m,
                    {
                        "metadata": {
                            "name": m.get("metadata").get(
                                "name",
                                "-".join(
                                    [
                                        i
                                        for i in [
                                            recursive_get(
                                                m,
                                                f"metadata#labels#{await self.labels_name}",
                                                sep="#",
                                            ),
                                            recursive_get(
                                                m,
                                                f"metadata#labels#{await self.labels_instance}",
                                                sep="#",
                                            ),
                                            cheap_hash(
                                                recursive_get(
                                                    m,
                                                    f"metadata#labels#{await self.labels_domain_name}",
                                                    sep="#",
                                                )
                                            ),
                                            recursive_get(
                                                m,
                                                f"metadata#labels#{await self.labels_component}",
                                                sep="#",
                                            ),
                                        ]
                                        if i
                                    ]
                                ),
                            )
                        }
                    },
                )
                for m in [
                    merge(
                        {i: x[i] for i in x if i in ["apiVersion", "kind", "metadata"]},
                        r,
                    )
                    for x in await getattr(self, f"{(await self.scope).lower()}s")
                    for r in yaml.load_all(
                        self.jinja.render(
                            recursive_get(await self.template, "spec.template"),
                            parent=(await self.parent),
                            child=(await self.child),
                            namespace=recursive_get(x, "metadata.namespace"),
                            name=recursive_get(
                                x, f"metadata#labels#{await self.labels_name}", sep="#"
                            ),
                            instance=recursive_get(
                                x,
                                f"metadata#labels#{await self.labels_instance}",
                                sep="#",
                            ),
                            domain_name=recursive_get(
                                x,
                                f"metadata#labels#{await self.labels_domain_name}",
                                sep="#",
                            ),
                            component=recursive_get(
                                x,
                                f"metadata#labels#{await self.labels_component}",
                                sep="#",
                            ),
                            managed_by=recursive_get(
                                x,
                                f"metadata#labels#{await self.labels_managed_by}",
                                sep="#",
                            ),
                            labels_component=await self.labels_component,
                            labels_domain_name=await self.labels_domain_name,
                            labels_instance=await self.labels_instance,
                            labels_managed_by=await self.labels_managed_by,
                            labels_name=await self.labels_name,
                            **(await self.items),
                            **x,
                        )
                        or "",
                        Loader=yaml.FullLoader,
                    )
                ]
            ]
            if self.dump:
                with open(
                    os.path.join(self.dump, f"alfatemplate-{self.name}-renders.yaml"),
                    "w",
                ) as outfile:
                    outfile.write(yaml.dump_all(self._renders))
            logger.info(f" - Got {len(self._renders)} Renders")
        return self._renders

    @property
    async def owner_references(self):
        return recursive_get(await self.template, "spec.ownerReferences")

    @property
    async def component(self):
        return recursive_get(await self.template, "spec.component")

    @property
    async def labels_name(self):
        return recursive_get(await self.template, "spec.labels.name")

    @property
    async def labels_instance(self):
        return recursive_get(await self.template, "spec.labels.instance")

    @property
    async def labels_domain_name(self):
        return recursive_get(await self.template, "spec.labels.domainName")

    @property
    async def labels_component(self):
        return recursive_get(await self.template, "spec.labels.component")

    @property
    async def labels_managed_by(self):
        return recursive_get(await self.template, "spec.labels.managedBy")

    @property
    async def kinds(self):
        return [(await self.parent_kind), *(await self.monitored_kinds)]

    @property
    async def child(self):
        return self.api.kinds[await self.child_kind]

    @property
    async def child_kind(self):
        return recursive_get(await self.template, "spec.kinds.child.kind")

    @property
    async def scope(self):
        return recursive_get(await self.template, "spec.scope")

    @property
    async def parent(self):
        return self.api.kinds[await self.parent_kind]

    @property
    async def parent_kind(self):
        return recursive_get(await self.template, "spec.kinds.parent.kind")

    @property
    async def monitored_kinds(self):
        return [
            k["kind"]
            for k in recursive_get(await self.template, "spec.kinds.monitored")
        ]

    @property
    async def template(self):
        if "_template" not in self.__dict__ or self._template is None:
            logger.info("Getting Template")
            self._template = [
                t
                for t in await self.get_items("AlfaTemplate")
                if t["metadata"]["name"] == self.name
            ]
            if self.dump:
                with open(
                    os.path.join(
                        self.dump,
                        f"alfatemplate-{self.name}-template.yaml",
                    ),
                    "w",
                ) as outfile:
                    outfile.write(yaml.dump_all(self._template))
            logger.info(f" - Got {len(self._template)} Template(s)")
        return first(self._template)

    async def get_items(self, kind):
        items = []
        async with self.session.request(
            "get", self.api.kinds[kind].rest_path
        ) as item_collection_response:
            item_collection = await item_collection_response.json()
            for item in item_collection["items"]:
                async with self.session.request(
                    "get",
                    URL(
                        self.api.kinds[kind].calculate_url(
                            item["metadata"].get("namespace", None),
                            item["metadata"]["name"],
                        )
                    ),
                ) as item_response:
                    item = await item_response.json()
                    items.append(item)
        return items
